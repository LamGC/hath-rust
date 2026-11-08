//! Exercise the actual CLI before credentials, logger, directories or EH RPC initialization.
#![cfg(target_os = "linux")]

use std::process::Command;

#[test]
fn help_exits_without_starting_a_client_or_creating_runtime_directories() {
    let root = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_hath-rust"))
        .current_dir(root.path())
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    let help = String::from_utf8(output.stdout).unwrap();
    assert_eq!(help.contains("--manager-describe"), cfg!(feature = "manager-integration"));
    assert_eq!(help.contains("--manager-config"), cfg!(feature = "manager-integration"));
}

#[test]
fn invalid_rpc_bootstrap_is_an_admission_error_not_a_lifecycle_panic() {
    let root = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_hath-rust"))
        .current_dir(root.path())
        .args(["--rpc-server-ip", "["])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("Invalid RPC bootstrap address"));
    assert!(!error.contains("panicked"));
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[cfg(feature = "manager-integration")]
#[test]
fn offline_descriptor_hashes_the_actual_binary_without_reading_configuration() {
    use aws_lc_rs::digest;
    use std::io::Read;
    let executable = env!("CARGO_BIN_EXE_hath-rust");
    let root = tempfile::tempdir().unwrap();
    let output = Command::new(executable)
        .current_dir(root.path())
        .args(["--manager-describe", "--json", "--manager-config", "missing-private-config"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(output.stderr.is_empty());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["schemaVersion"], 1);
    assert_eq!(value["recoveryAbi"], 1);
    assert_eq!(value["peerKind"], "rust_native");
    assert_eq!(value["launchSupported"], true); // Linux startup and recovery paths have executable acceptance tests.
    assert_eq!(value["protocol"]["major"], 1);
    assert_eq!(value["protocol"]["minor"], 0);
    assert_eq!(value["identity"]["implementationVersion"], env!("CARGO_PKG_VERSION"));
    let mut file = std::fs::File::open(executable).unwrap();
    let mut hasher = digest::Context::new(&digest::SHA256);
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    assert_eq!(value["identity"]["artifactSha256"], const_hex::encode(hasher.finish()));
}

#[cfg(feature = "manager-integration")]
#[test]
fn missing_native_configuration_is_rejected_before_any_client_side_effect() {
    let root = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_hath-rust"))
        .current_dir(root.path())
        .args(["--manager-config", "missing-private-config"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!output.stderr.is_empty());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[cfg(feature = "manager-integration")]
struct ManagedFixture {
    _root: tempfile::TempDir,
    data: std::path::PathBuf,
    spool: std::path::PathBuf,
    config: std::path::PathBuf,
    children: Vec<std::path::PathBuf>,
}
#[cfg(feature = "manager-integration")]
impl ManagedFixture {
    fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let data = root.path().join("data");
        let spool = root.path().join("spool");
        for path in [&data, &spool] {
            std::fs::create_dir(path).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let token = root.path().join("token");
        let config = root.path().join("runtime.json");
        for (path, bytes) in [
            (&token, "A".repeat(43)),
            (&data.join("client_login"), "1001-BBBBBBBBBBBBBBBBBBBB".into()),
            (
                &config,
                serde_json::json!({"instanceId":"11111111-1111-4111-8111-111111111111","stableInstanceKey":"primary",
                "rpcUrl":"http://127.0.0.1:1/","credentialFile":token,"spoolDirectory":spool})
                .to_string(),
            ),
        ] {
            std::fs::write(path, bytes).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let children = ["cache", "temp", "log", "download"]
            .iter()
            .map(|name| root.path().join(name))
            .collect();
        Self {
            _root: root,
            data,
            spool,
            config,
            children,
        }
    }
    fn command(&self) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_hath-rust"));
        cmd.current_dir(self._root.path())
            .arg("--manager-config")
            .arg(&self.config)
            .arg("--data-dir")
            .arg(&self.data);
        for (flag, path) in ["--cache-dir", "--temp-dir", "--log-dir", "--download-dir"]
            .iter()
            .zip(&self.children)
        {
            cmd.arg(flag).arg(path);
        }
        cmd
    }
}

#[cfg(feature = "manager-integration")]
#[test]
fn managed_cli_never_falls_back_to_interactive_setup_or_creates_client_dirs_for_invalid_credentials() {
    let f = ManagedFixture::new();
    for credential in [Some("1001-secret-marker"), None] {
        let login = f.data.join("client_login");
        match credential {
            Some(value) => std::fs::write(&login, value).unwrap(),
            None => std::fs::remove_file(&login).unwrap(),
        }
        let output = f.command().output().unwrap();
        assert!(!output.status.success());
        let all = format!("{}{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        assert!(!all.contains("Enter Client"));
        assert!(!all.contains("secret-marker"));
        assert!(f.children.iter().all(|path| !path.exists()));
        assert_eq!(std::fs::read_dir(&f.spool).unwrap().count(), 0);
    }
}

#[cfg(feature = "manager-integration")]
#[test]
fn actual_cli_handles_sigterm_while_login_is_waiting_and_releases_writer_locks() {
    use std::{
        io::Read,
        process::{Child, Stdio},
        time::{Duration, Instant},
    };
    struct ChildGuard(Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let f = ManagedFixture::new();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let mut child = ChildGuard(
        f.command()
            .args(["--rpc-server-ip", &address])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut stream = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
            Err(e) => panic!("{e}"),
        }
        assert!(Instant::now() < deadline, "startup never reached isolated login");
        assert!(child.0.try_wait().unwrap().is_none(), "client exited before mock login");
        std::thread::sleep(Duration::from_millis(10));
    };
    stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        let mut b = [0];
        stream.read_exact(&mut b).unwrap();
        request.push(b[0]);
        assert!(request.len() < 8192);
    }
    assert!(String::from_utf8(request).unwrap().contains("act=server_stat"));
    // Keep the mock connection open and nonresponsive until the child is gone.
    let pid = rustix::process::Pid::from_raw(child.0.id() as i32).unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::TERM).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "SIGTERM did not cancel startup");
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success());
    for path in [&f.data, &f.spool] {
        let fd = std::fs::File::open(path).unwrap();
        rustix::fs::flock(&fd, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
    }
}

#[cfg(feature = "manager-integration")]
#[test]
fn offline_recovery_refuses_another_writer_and_cannot_create_a_run_directory() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let lock = std::fs::File::open(root.path()).unwrap();
    rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_hath-rust"))
        .arg("--manager-recover-spool")
        .arg(root.path())
        .args(["--manager-instance-id", "11111111-1111-4111-8111-111111111111"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[cfg(feature = "manager-integration")]
#[test]
fn offline_spool_recovery_retains_corrupt_bytes_and_reports_quarantine() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let instance = "11111111-1111-4111-8111-111111111111";
    let directory = root.path().join(instance);
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let bad = directory.join("bad.json");
    std::fs::write(&bad, b"bad").unwrap();
    std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o600)).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_hath-rust"))
        .arg("--manager-recover-spool")
        .arg(root.path())
        .args(["--manager-instance-id", instance])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["quarantined"], 1);
    let quarantined: Vec<_> = std::fs::read_dir(root.path().join("quarantine")).unwrap().collect();
    assert_eq!(quarantined.len(), 1);
    assert_eq!(std::fs::read(quarantined[0].as_ref().unwrap().path()).unwrap(), b"bad");
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
}
