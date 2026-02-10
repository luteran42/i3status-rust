use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::Duration;

use pipewire::spa::{
    param::ParamType,
    pod::{Object, Property, PropertyFlags, Value, ValueArray, deserialize::PodDeserializer},
    sys,
};
use pipewire::{
    context::Context, keys, main_loop::MainLoop, properties::properties, types::ObjectType,
};
use serde_json::Value as JsonValue;
use tokio::sync::Notify;

use super::super::prelude::*;
use super::{DeviceKind, SoundDevice};

static CLIENT: LazyLock<Result<Client>> = LazyLock::new(Client::new);
static EVENT_LISTENER: Mutex<Vec<Weak<Notify>>> = Mutex::new(Vec::new());
static DEVICES: LazyLock<Mutex<HashMap<u32, VolInfo>>> = LazyLock::new(default);
static DEFAULTS: LazyLock<Mutex<Defaults>> = LazyLock::new(default);

#[derive(Debug, Default)]
struct Defaults {
    sink: Option<String>,
    source: Option<String>,
}

#[derive(Debug, Clone)]
struct VolInfo {
    id: u32,
    kind: DeviceKind,
    name: String,
    description: Option<String>,
    volume_avg: u32,
    mute: bool,
    channel_volumes: Vec<f32>,
}

struct NodeHandle {
    node: pipewire::node::Node,
    _listener: pipewire::node::NodeListener,
    kind: DeviceKind,
    name: String,
    description: Option<String>,
}

#[derive(Debug)]
enum ClientRequest {
    SetVolume {
        id: u32,
        volumes: Vec<f32>,
        mute: Option<bool>,
    },
    SetMute {
        id: u32,
        mute: bool,
    },
}

struct Client {
    send_req: std::sync::mpsc::Sender<ClientRequest>,
}

impl Client {
    fn new() -> Result<Client> {
        let (send_req, recv_req) = std::sync::mpsc::channel::<ClientRequest>();

        thread::Builder::new()
            .name("sound_pipewire".to_string())
            .spawn(move || Self::main_loop_thread(recv_req))
            .error("failed to spawn pipewire thread")?;

        Ok(Client { send_req })
    }

    fn send(request: ClientRequest) -> Result<()> {
        match CLIENT.as_ref() {
            Ok(client) => {
                client.send_req.send(request).unwrap();
                Ok(())
            }
            Err(err) => Err(Error::new(format!(
                "pipewire connection failed with error: {err}",
            ))),
        }
    }

    fn send_update_event() {
        EVENT_LISTENER
            .lock()
            .unwrap()
            .retain(|notify| notify.upgrade().inspect(|x| x.notify_one()).is_some());
    }

    fn main_loop_thread(recv_req: std::sync::mpsc::Receiver<ClientRequest>) {
        let proplist = properties! {*keys::APP_NAME => env!("CARGO_PKG_NAME")};
        let main_loop = MainLoop::new(None).expect("Failed to create main loop");
        let context =
            Context::with_properties(&main_loop, proplist).expect("Failed to create context");
        let core = context.connect(None).expect("Failed to connect");
        let registry = Rc::new(core.get_registry().expect("Failed to get registry"));

        let updated = Rc::new(Cell::new(false));
        let updated_copy = updated.clone();
        let updated_copy2 = updated.clone();

        let nodes: Rc<RefCell<HashMap<u32, NodeHandle>>> = Rc::new(RefCell::new(HashMap::new()));
        let nodes_cb = nodes.clone();
        let metadata_listeners: Rc<RefCell<Vec<pipewire::metadata::MetadataListener>>> =
            Rc::new(RefCell::new(Vec::new()));
        let metadata_listeners_cb = metadata_listeners.clone();

        // Clone registry for use inside the closure
        let registry_clone = registry.clone();

        let _registry_listener = registry
            .add_listener_local()
            .global(move |global| {
                let Some(global_props) = global.props else {
                    return;
                };
                match &global.type_ {
                    ObjectType::Node => {
                        let media_class = global_props.get(&keys::MEDIA_CLASS);
                        let kind = match media_class {
                            Some("Audio/Sink") => DeviceKind::Sink,
                            Some("Audio/Source") => DeviceKind::Source,
                            _ => return,
                        };

                        // Capture global.id early to avoid lifetime issues
                        let global_id = global.id;

                        let name = global_props
                            .get(&keys::NODE_NAME)
                            .map_or_else(|| format!("node_{}", global_id), |s| s.to_string());
                        let description = global_props
                            .get(&keys::NODE_DESCRIPTION)
                            .map(|s| s.to_string());

                        let Ok(node) = registry_clone.bind::<pipewire::node::Node, _>(global)
                        else {
                            return;
                        };

                        // Clone values for use in the closure
                        let updated_copy_inner = updated_copy.clone();
                        let nodes_cb_inner = nodes_cb.clone();
                        let name_inner = name.clone();
                        let description_inner = description.clone();

                        let listener: pipewire::node::NodeListener =
                            node.add_listener_local()
                                .param(move |_, id, _, _, param| {
                                    if id != ParamType::Props {
                                        return;
                                    }
                                    let Some(param) = param else {
                                        return;
                                    };
                                    if let Some((volumes, mute)) = parse_props(param) {
                                        let avg = volume_avg(&volumes);
                                        let mut devices = DEVICES.lock().unwrap();
                                        devices
                                            .entry(global_id)
                                            .and_modify(|info| {
                                                info.volume_avg = avg;
                                                info.mute = mute;
                                                info.channel_volumes = volumes.clone();
                                            })
                                            .or_insert_with(|| VolInfo {
                                                id: global_id,
                                                kind,
                                                name: name_inner.clone(),
                                                description: description_inner.clone(),
                                                volume_avg: avg,
                                                mute,
                                                channel_volumes: volumes.clone(),
                                            });
                                    }
                                    nodes_cb_inner.borrow_mut().entry(global_id).and_modify(
                                        |info| {
                                            info.name = name_inner.clone();
                                            info.description = description_inner.clone();
                                            info.kind = kind;
                                        },
                                    );
                                    updated_copy_inner.set(true);
                                })
                                .register();

                        node.subscribe_params(&[ParamType::Props]);
                        node.enum_params(0, Some(ParamType::Props), 0, u32::MAX);

                        nodes_cb.borrow_mut().insert(
                            global_id,
                            NodeHandle {
                                node,
                                _listener: listener,
                                kind,
                                name: name.clone(),
                                description: description.clone(),
                            },
                        );

                        DEVICES.lock().unwrap().entry(global_id).or_insert(VolInfo {
                            id: global_id,
                            kind,
                            name,
                            description,
                            volume_avg: 0,
                            mute: false,
                            channel_volumes: Vec::new(),
                        });

                        updated_copy.set(true);
                    }
                    ObjectType::Metadata => {
                        let Some(meta_name) = global_props.get("metadata.name") else {
                            return;
                        };
                        if meta_name != "default" {
                            return;
                        }

                        let Ok(metadata) =
                            registry_clone.bind::<pipewire::metadata::Metadata, _>(global)
                        else {
                            return;
                        };

                        let updated_copy = updated_copy.clone();
                        let metadata_listeners_cb = metadata_listeners_cb.clone();
                        let _listener: pipewire::metadata::MetadataListener = metadata
                            .add_listener_local()
                            .property(move |subject, key, _, value| {
                                if subject != 0 {
                                    return 0;
                                }
                                let Some(value) = value else {
                                    return 0;
                                };
                                let name = parse_default_name(value);
                                let mut defaults = DEFAULTS.lock().unwrap();
                                match key {
                                    Some("default.audio.sink") => defaults.sink = Some(name),
                                    Some("default.audio.source") => defaults.source = Some(name),
                                    _ => (),
                                }
                                updated_copy.set(true);
                                0
                            })
                            .register();

                        metadata_listeners_cb.borrow_mut().push(_listener);
                    }
                    _ => (),
                }
            })
            .global_remove(move |uid| {
                let mut devices = DEVICES.lock().unwrap();
                if devices.remove(&uid).is_some() {
                    updated_copy2.set(true);
                }
            })
            .register();

        loop {
            main_loop.loop_().iterate(Duration::from_millis(200));

            while let Ok(req) = recv_req.try_recv() {
                match req {
                    ClientRequest::SetVolume { id, volumes, mute } => {
                        if let Some(handle) = nodes.borrow().get(&id)
                            && let Ok(bytes) = build_props_bytes(&volumes, mute)
                            && let Some(pod) = pipewire::spa::pod::Pod::from_bytes(&bytes)
                        {
                            handle.node.set_param(ParamType::Props, 0, pod);
                        }
                    }
                    ClientRequest::SetMute { id, mute } => {
                        if let Some(handle) = nodes.borrow().get(&id)
                            && let Ok(bytes) = build_props_bytes(&[], Some(mute))
                            && let Some(pod) = pipewire::spa::pod::Pod::from_bytes(&bytes)
                        {
                            handle.node.set_param(ParamType::Props, 0, pod);
                        }
                    }
                }
            }

            if updated.get() {
                updated.set(false);
                Client::send_update_event();
            }
        }
    }
}

fn parse_default_name(value: &str) -> String {
    if let Ok(json) = serde_json::from_str::<JsonValue>(value)
        && let Some(name) = json.get("name").and_then(|v| v.as_str())
    {
        return name.to_string();
    }
    value.to_string()
}

fn volume_avg(volumes: &[f32]) -> u32 {
    if volumes.is_empty() {
        return 0;
    }
    let sum: f32 = volumes.iter().copied().sum();
    // Convert linear volume to cubic (human) volume: cbrt(linear) * 100
    ((sum / volumes.len() as f32).cbrt() * 100.0).round() as u32
}

fn parse_props(param: &pipewire::spa::pod::Pod) -> Option<(Vec<f32>, bool)> {
    let ptr = std::ptr::NonNull::new(param.as_raw_ptr())?;
    let value = unsafe { PodDeserializer::deserialize_ptr::<Value>(ptr).ok()? };

    let Value::Object(obj) = value else {
        return None;
    };

    let mut volumes: Option<Vec<f32>> = None;
    let mut mute: Option<bool> = None;

    for prop in obj.properties {
        match prop.key {
            sys::SPA_PROP_channelVolumes => {
                if let Value::ValueArray(ValueArray::Float(vals)) = prop.value {
                    volumes = Some(vals);
                }
            }
            sys::SPA_PROP_volume => {
                if let Value::Float(val) = prop.value {
                    volumes = Some(vec![val]);
                }
            }
            sys::SPA_PROP_mute => {
                if let Value::Bool(val) = prop.value {
                    mute = Some(val);
                }
            }
            _ => (),
        }
    }

    if volumes.is_none() && mute.is_none() {
        return None;
    }

    Some((volumes.unwrap_or_default(), mute.unwrap_or(false)))
}

fn build_props_bytes(volumes: &[f32], mute: Option<bool>) -> Result<Vec<u8>> {
    use pipewire::spa::pod::serialize::PodSerializer;

    let mut properties = Vec::new();

    if let Some(mute) = mute {
        properties.push(Property {
            key: sys::SPA_PROP_mute,
            flags: PropertyFlags::empty(),
            value: Value::Bool(mute),
        });
    }

    if !volumes.is_empty() {
        properties.push(Property {
            key: sys::SPA_PROP_channelVolumes,
            flags: PropertyFlags::empty(),
            value: Value::ValueArray(ValueArray::Float(volumes.to_vec())),
        });
    }

    // In pipewire 0.8, we build the object value directly
    // SPA_TYPE_OBJECT_Props is the correct type for Props objects
    let value = Value::Object(Object {
        type_: sys::SPA_TYPE_OBJECT_Props,
        id: sys::SPA_PARAM_Props,
        properties,
    });

    let cursor = std::io::Cursor::new(Vec::new());
    let (cursor, _) =
        PodSerializer::serialize(cursor, &value).error("Failed to serialize PipeWire props")?;
    Ok(cursor.into_inner())
}

pub(super) struct Device {
    target: DeviceTarget,
    device_kind: DeviceKind,
    volume_avg: u32,
    muted: bool,
    output_name: String,
    output_description: Option<String>,
    channel_volumes: Vec<f32>,
    notify: Arc<Notify>,
}

#[derive(Debug, Clone)]
enum DeviceTarget {
    Default,
    ById(u32),
    ByName(String),
}

impl DeviceTarget {
    fn from_name(name: Option<String>) -> Self {
        match name {
            None => DeviceTarget::Default,
            Some(s) => match s.parse::<u32>() {
                Ok(id) => DeviceTarget::ById(id),
                Err(_) => DeviceTarget::ByName(s),
            },
        }
    }
}

impl Device {
    pub(super) fn new(device_kind: DeviceKind, name: Option<String>) -> Result<Self> {
        // Check if PipeWire client is available
        CLIENT
            .as_ref()
            .map_err(|e| Error::new(format!("PipeWire not available: {e}")))?;

        let notify = Arc::new(Notify::new());
        EVENT_LISTENER.lock().unwrap().push(Arc::downgrade(&notify));

        Ok(Device {
            target: DeviceTarget::from_name(name),
            device_kind,
            volume_avg: 0,
            muted: false,
            output_name: String::new(),
            output_description: None,
            channel_volumes: Vec::new(),
            notify,
        })
    }

    fn resolve_info(&self) -> Option<VolInfo> {
        let devices = DEVICES.lock().unwrap();

        match &self.target {
            DeviceTarget::ById(id) => devices.get(id).cloned(),
            DeviceTarget::ByName(name) => devices
                .values()
                .find(|info| info.kind == self.device_kind && info.name == *name)
                .cloned(),
            DeviceTarget::Default => {
                let defaults = DEFAULTS.lock().unwrap();
                let default_name = match self.device_kind {
                    DeviceKind::Sink => defaults.sink.as_ref(),
                    DeviceKind::Source => defaults.source.as_ref(),
                };

                if let Some(default_name) = default_name {
                    devices
                        .values()
                        .find(|info| info.kind == self.device_kind && &info.name == default_name)
                        .cloned()
                } else {
                    devices
                        .values()
                        .find(|info| info.kind == self.device_kind)
                        .cloned()
                }
            }
        }
    }

    fn resolve_id(&self) -> Option<u32> {
        self.resolve_info().map(|info| info.id)
    }
}

#[async_trait::async_trait]
impl SoundDevice for Device {
    fn volume(&self) -> u32 {
        self.volume_avg
    }

    fn muted(&self) -> bool {
        self.muted
    }

    fn output_name(&self) -> String {
        self.output_name.clone()
    }

    fn output_description(&self) -> Option<String> {
        self.output_description.clone()
    }

    fn active_port(&self) -> Option<String> {
        None
    }

    fn form_factor(&self) -> Option<&str> {
        None
    }

    async fn get_info(&mut self) -> Result<()> {
        // Wait for PipeWire to enumerate devices (up to 2 seconds)
        let mut retries = 0;
        let info = loop {
            if let Some(info) = self.resolve_info() {
                break info;
            }

            retries += 1;
            if retries > 20 {
                return Err(Error::new(format!(
                    "PipeWire device not found after {} retries. Available devices: {:?}",
                    retries,
                    DEVICES
                        .lock()
                        .unwrap()
                        .values()
                        .filter(|d| d.kind == self.device_kind)
                        .map(|d| format!("{}:{}", d.id, d.name))
                        .collect::<Vec<_>>()
                )));
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        };

        self.volume_avg = info.volume_avg;
        self.muted = info.mute;
        self.output_name = info.name;
        self.output_description = info.description;
        self.channel_volumes = info.channel_volumes;

        Ok(())
    }

    async fn set_volume(&mut self, step: i32, max_vol: Option<u32>) -> Result<()> {
        let id = self.resolve_id().error("PipeWire device not found")?;

        let new_vol = (self.volume_avg as i32 + step).max(0) as u32;
        let capped = if let Some(max_vol) = max_vol {
            new_vol.min(max_vol)
        } else {
            new_vol
        };

        let channel_count = if self.channel_volumes.is_empty() {
            2
        } else {
            self.channel_volumes.len()
        };

        // Convert cubic percentage back to linear for PipeWire
        let cubic_vol = capped as f32 / 100.0;
        let linear_vol = cubic_vol.powi(3);

        let new_volumes = vec![linear_vol; channel_count];

        self.volume_avg = capped;
        self.channel_volumes = new_volumes.clone();

        Client::send(ClientRequest::SetVolume {
            id,
            volumes: new_volumes,
            mute: None,
        })?;

        Ok(())
    }

    async fn toggle(&mut self) -> Result<()> {
        let id = self.resolve_id().error("PipeWire device not found")?;
        self.muted = !self.muted;

        Client::send(ClientRequest::SetMute {
            id,
            mute: self.muted,
        })?;

        Ok(())
    }

    async fn wait_for_update(&mut self) -> Result<()> {
        self.notify.notified().await;
        Ok(())
    }
}
