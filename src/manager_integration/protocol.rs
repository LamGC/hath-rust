use serde::{
    Deserialize, Serialize,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Value};
use std::collections::HashSet;

pub(super) const PEER_KIND: &str = "rust_native";
pub(super) const PROFILE: &str = "gallery-v2";
pub(super) const REQUIRED: [&str; 5] = [
    "events.reliable.v1",
    "gallery.lifecycle.v2",
    "gallery.metadata.v2",
    "client.native.v1",
    "native.health.v1",
];
pub(super) const MAX_RESPONSE: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct NativeHealth {
    pub health_sequence: i64,
    pub observed_at: chrono::DateTime<chrono::Utc>,
    pub integration_ready: bool,
    pub persistence_healthy: bool,
    pub client_ready: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct NativeIdentity {
    pub implementation_version: String,
    pub integration_version: String,
    pub build_commit: String,
    pub target: String,
    pub artifact_sha256: String,
    pub eh_compatibility_version: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Offer {
    pub major: u16,
    pub minor: u16,
    pub capabilities: Vec<String>,
    pub required_capabilities: Vec<String>,
    pub output_profile: String,
}

impl Offer {
    pub fn native() -> Self {
        Self {
            major: 1,
            minor: 0,
            capabilities: REQUIRED.into_iter().chain(["telemetry.v1"]).map(str::to_owned).collect(),
            required_capabilities: REQUIRED.map(str::to_owned).to_vec(),
            output_profile: PROFILE.into(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Registration<'a> {
    pub instance_id: &'a str,
    pub agent_version: &'a str,
    pub java_version: Option<&'a str>,
    pub hath_version: &'a str,
    pub hath_build: Option<i32>,
    pub adapter_id: &'a str,
    pub peer_kind: &'a str,
    pub native_identity: &'a NativeIdentity,
    pub protocol: Offer,
}

impl<'a> Registration<'a> {
    pub fn new(instance_id: &'a str, identity: &'a NativeIdentity) -> Self {
        Self {
            instance_id,
            agent_version: &identity.integration_version,
            java_version: None,
            hath_version: &identity.implementation_version,
            hath_build: None,
            adapter_id: super::ADAPTER_ID,
            peer_kind: PEER_KIND,
            native_identity: identity,
            protocol: Offer::native(),
        }
    }
}

#[derive(Deserialize)]
pub(super) struct Version {
    major: u16,
    minor: u16,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Acceptance {
    status: String,
    server_time: chrono::DateTime<chrono::Utc>,
    manager_protocol: Version,
    selected_protocol: Version,
    enabled_capabilities: Vec<String>,
    output_profile: String,
    session_id: String,
    legacy: bool,
}

pub(super) struct Session {
    pub id: String,
    pub capabilities: HashSet<String>,
}

impl Acceptance {
    pub fn validate(self) -> Result<Session, &'static str> {
        let offered = Offer::native();
        let names: HashSet<_> = self.enabled_capabilities.iter().cloned().collect();
        if self.status != "registered"
            || self.legacy
            || self.manager_protocol.major != 1
            || self.selected_protocol.major != 1
            || self.selected_protocol.minor != 0
            || self.output_profile != PROFILE
            || self.session_id.len() != 32
            || !self.session_id.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
            || self.enabled_capabilities.len() > 64
            || names.len() != self.enabled_capabilities.len()
            || names.iter().any(|name| !offered.capabilities.contains(name))
            || REQUIRED.iter().any(|name| !names.contains(*name))
        {
            return Err("manager_protocol_rejected");
        }
        Ok(Session {
            id: self.session_id,
            capabilities: names,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Ack {
    pub ok: bool,
    pub batch_id: Option<String>,
}

pub(super) fn acknowledge(bytes: &[u8], batch_id: Option<&str>) -> Result<(), &'static str> {
    let value: Ack = decode(bytes)?;
    if !value.ok || batch_id.is_some() && value.batch_id.as_deref() != batch_id {
        return Err("manager_invalid_ack");
    }
    Ok(())
}

// Validate duplicate names and depth before typed deserialization. Value alone would silently
// replace duplicate object members; errors must not expose server response bodies.
pub(super) fn decode<T: de::DeserializeOwned>(bytes: &[u8]) -> Result<T, &'static str> {
    decode_limit(bytes, MAX_RESPONSE)
}

pub(super) fn decode_limit<T: de::DeserializeOwned>(bytes: &[u8], maximum: usize) -> Result<T, &'static str> {
    if bytes.is_empty() || bytes.len() > maximum {
        return Err("manager_response_size");
    }
    let mut parser = serde_json::Deserializer::from_slice(bytes);
    let value = JsonSeed(0).deserialize(&mut parser).map_err(|_| "manager_invalid_json")?;
    parser.end().map_err(|_| "manager_invalid_json")?;
    serde_json::from_value(value).map_err(|_| "manager_invalid_json")
}

struct JsonSeed(usize);
impl<'de> DeserializeSeed<'de> for JsonSeed {
    type Value = Value;
    fn deserialize<D: de::Deserializer<'de>>(self, parser: D) -> Result<Value, D::Error> {
        if self.0 > 16 {
            return Err(de::Error::custom("depth"));
        }
        parser.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for JsonSeed {
    type Value = Value;
    fn expecting(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_str("bounded JSON")
    }
    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Value, E> {
        Ok(value.into())
    }
    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Value, E> {
        Ok(value.into())
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Value, E> {
        Ok(value.into())
    }
    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Value, E> {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("number"))
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<Value, E> {
        Ok(value.into())
    }
    fn visit_string<E: de::Error>(self, value: String) -> Result<Value, E> {
        Ok(value.into())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut input: A) -> Result<Value, A::Error> {
        let mut map = Map::new();
        while let Some(key) = input.next_key::<String>()? {
            if map.contains_key(&key) {
                return Err(de::Error::custom("duplicate"));
            }
            let value = input.next_value_seed(JsonSeed(self.0 + 1))?;
            map.insert(key, value);
        }
        Ok(Value::Object(map))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut input: A) -> Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = input.next_element_seed(JsonSeed(self.0 + 1))? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn serializer_matches_manager_fixture() {
        let provenance: Value = serde_json::from_str(include_str!("fixtures/provenance.json")).unwrap();
        assert_eq!(
            provenance["sha256"],
            const_hex::encode(aws_lc_rs::digest::digest(
                &aws_lc_rs::digest::SHA256,
                include_bytes!("fixtures/agent-registration-native-v1.json")
            ))
        );
        let expected: Value = serde_json::from_str(include_str!("fixtures/agent-registration-native-v1.json")).unwrap();
        let identity: NativeIdentity = serde_json::from_value(expected["nativeIdentity"].clone()).unwrap();
        assert_eq!(serde_json::to_value(Registration::new("run-native", &identity)).unwrap(), expected);
    }
    #[test]
    fn ack_is_strict_and_batch_bound() {
        assert!(acknowledge(br#"{"ok":true,"batchId":"b"}"#, Some("b")).is_ok());
        for body in [
            br#"{"ok":"true"}"#.as_slice(),
            br#"{"ok":true,"ok":false}"#,
            br#"{"ok":true} trailing"#,
            br#"null"#,
            br#"{"ok":true,"batchId":"different"}"#,
            br#"{"ok":false,"batchId":"b"}"#,
        ] {
            assert!(acknowledge(body, Some("b")).is_err());
        }
    }
    #[test]
    fn limits_and_nested_duplicates_are_enforced() {
        assert!(decode::<Value>(&vec![b' '; MAX_RESPONSE + 1]).is_err());
        assert!(decode::<Value>(br#"{"a":{"b":1,"b":2}}"#).is_err());
        let nested = format!("{}0{}", "[".repeat(18), "]".repeat(18));
        assert!(decode::<Value>(nested.as_bytes()).is_err());
        assert!(decode::<Version>(br#"{"major":1.0,"minor":0}"#).is_err());
    }

    #[test]
    fn negotiation_requires_native_capabilities_and_bounded_version_and_session() {
        let original = serde_json::json!({"status":"registered","serverTime":"2026-11-07T00:00:00Z",
            "managerProtocol":{"major":1,"minor":20},"selectedProtocol":{"major":1,"minor":0},
            "enabledCapabilities":Offer::native().capabilities,"outputProfile":PROFILE,
            "sessionId":"0123456789abcdef0123456789abcdef","legacy":false});
        assert!(
            decode::<Acceptance>(&serde_json::to_vec(&original).unwrap())
                .unwrap()
                .validate()
                .is_ok()
        );
        for (name, value) in [
            ("legacy", serde_json::json!(true)),
            ("sessionId", serde_json::json!("bad")),
            ("enabledCapabilities", serde_json::json!(["injection.health.v1"])),
            ("outputProfile", serde_json::json!("other")),
        ] {
            let mut invalid = original.clone();
            invalid[name] = value;
            assert!(
                decode::<Acceptance>(&serde_json::to_vec(&invalid).unwrap())
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
    }
}
