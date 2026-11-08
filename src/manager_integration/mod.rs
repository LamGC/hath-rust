//! Optional, outbound-only hath-manager integration. Business code depends on this façade,
//! never on JSON, bearer credentials or the persistence implementation.
#![allow(dead_code)] // Some aggregation entry points are reserved for the next observability batch.

mod config;
#[cfg(target_os = "linux")]
mod gallery;
#[cfg(target_os = "linux")]
mod http;
#[cfg(target_os = "linux")]
mod observation;
#[cfg(target_os = "linux")]
mod outbox;
#[cfg(target_os = "linux")]
mod owner;
mod protocol;
#[cfg(target_os = "linux")]
mod runtime;
#[cfg(target_os = "linux")]
mod telemetry;
mod transport;

pub(crate) use config::FailureCode;
#[cfg(target_os = "linux")]
pub(crate) use gallery::{FileReceipt, FileView, GalleryAttempt, GalleryView};
#[cfg(target_os = "linux")]
pub(crate) use http::{HttpObservation, ImageResponse, ImageSource};
#[cfg(target_os = "linux")]
pub(crate) use observation::{ObservationHandle, Operation, Outcome};
#[cfg(target_os = "linux")]
pub(crate) use owner::ManagedRuntime;
#[cfg(target_os = "linux")]
pub(crate) use runtime::IntegrationHandle;

use aws_lc_rs::digest;
use serde::Serialize;
use std::io::{self, Read};

pub(crate) const INTEGRATION_VERSION: &str = "1.0.0";
pub(crate) const ADAPTER_ID: &str = "hath-rust-native-v1";

#[cfg(target_os = "linux")]
pub(crate) fn recover_spool_for_parent(root: &std::path::Path, instance: &str) -> io::Result<()> {
    let quarantined = outbox::Outbox::recover_offline(root, instance)?;
    println!("{}", serde_json::json!({"schemaVersion":1,"instanceId":instance,"quarantined":quarantined}));
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn print_recovered_handoff(directory: &std::path::Path, instance: &str, root: &std::path::Path) -> io::Result<()> {
    let _lock = outbox::lock_recovery_root(root)?;
    let (initialized, terminal) = gallery::recover_handoff(directory, instance)?;
    let bytes = serde_json::to_vec(&serde_json::json!({"schemaVersion":1,"instanceId":instance,
        "initialized":initialized,"terminal":terminal}))
    .map_err(|_| io::Error::other("manager_recovery_output"))?;
    if bytes.len() > 64 * 1024 {
        return Err(io::Error::other("manager_recovery_output_limit"));
    }
    println!("{}", std::str::from_utf8(&bytes).map_err(|_| io::Error::other("manager_recovery_output"))?);
    Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Descriptor {
    schema_version: u16,
    recovery_abi: u8,
    peer_kind: &'static str,
    launch_supported: bool,
    identity: protocol::NativeIdentity,
    protocol: protocol::Offer,
}

fn identity() -> io::Result<protocol::NativeIdentity> {
    let mut executable = std::fs::File::open(std::env::current_exe()?)?;
    let mut hasher = digest::Context::new(&digest::SHA256);
    let mut buffer = [0; 64 * 1024];
    loop {
        let read = executable.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(protocol::NativeIdentity {
        implementation_version: env!("CARGO_PKG_VERSION").into(),
        integration_version: INTEGRATION_VERSION.into(),
        build_commit: env!("HATH_MANAGER_SOURCE_COMMIT").into(),
        target: env!("HATH_MANAGER_TARGET").into(),
        artifact_sha256: const_hex::encode(hasher.finish()),
        eh_compatibility_version: crate::COMPAT_VERSION.into(),
    })
}

pub(crate) fn print_descriptor(json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let descriptor = Descriptor {
        schema_version: 1,
        recovery_abi: if cfg!(target_os = "linux") { 1 } else { 0 },
        peer_kind: protocol::PEER_KIND,
        launch_supported: cfg!(target_os = "linux"),
        identity: identity()?,
        protocol: protocol::Offer::native(),
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&descriptor)?);
    } else {
        println!("hath-manager protocol 1.0; rust_native; integration {INTEGRATION_VERSION}; {}", descriptor.identity.target);
    }
    Ok(())
}

pub(super) fn new_id() -> io::Result<String> {
    let mut bytes = [0; 16];
    aws_lc_rs::rand::fill(&mut bytes).map_err(|_| io::Error::other("manager_random_unavailable"))?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex = const_hex::encode(bytes);
    Ok(format!("{}-{}-{}-{}-{}", &hex[..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..]))
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) async fn test_runtime(
    root: &std::path::Path,
    records: usize,
) -> (runtime::Runtime, tokio::net::TcpListener, std::path::PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let token = root.join("token");
    std::fs::write(&token, "A".repeat(43)).unwrap();
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
    let spool = root.join("spool");
    std::fs::create_dir(&spool).unwrap();
    std::fs::set_permissions(&spool, std::fs::Permissions::from_mode(0o700)).unwrap();
    let fixture: serde_json::Value = serde_json::from_str(include_str!("fixtures/agent-registration-native-v1.json")).unwrap();
    let identity = serde_json::from_value(fixture["nativeIdentity"].clone()).unwrap();
    let config = config::Config {
        instance_id: "11111111-1111-4111-8111-111111111111".into(),
        stable_instance_key: "primary".into(),
        rpc_url: format!("http://{}/", listener.local_addr().unwrap()),
        credential_file: token,
        spool_directory: spool.clone(),
        heartbeat_millis: 1000,
        snapshot_millis: 1000,
    };
    (
        runtime::Runtime::start_with_limits(
            config,
            identity,
            outbox::Limits {
                records,
                bytes: 64 * 1024 * 1024,
            },
        )
        .await
        .unwrap(),
        listener,
        spool,
    )
}
