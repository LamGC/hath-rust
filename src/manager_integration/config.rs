use serde::Deserialize;
use std::{
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Config {
    // Only the External startup coordinator may populate a durable boot identity.
    // Managed JSON cannot opt out of its process-bound admission using this field.
    #[serde(skip)]
    pub external: Option<ExternalConnection>,
    pub instance_id: String,
    pub stable_instance_key: String,
    pub rpc_url: String,
    pub credential_file: PathBuf,
    pub spool_directory: PathBuf,
    #[serde(default = "default_heartbeat")]
    pub heartbeat_millis: u64,
    #[serde(default = "default_snapshot")]
    pub snapshot_millis: u64,
}

#[derive(Clone)]
pub(super) struct ExternalConnection {
    pub identity: super::protocol::ExternalIdentity,
    pub allow_private_http: bool,
    pub ca_certificate_file: Option<PathBuf>,
}

fn default_heartbeat() -> u64 {
    30_000
}
fn default_snapshot() -> u64 {
    10_000
}

// Deliberately neither Debug nor Serialize: errors/logs must never expose bearer material.
pub(super) struct Token(String);
impl Token {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Config {
    pub fn read(path: &Path) -> io::Result<Self> {
        let bytes = read_private(path, 64 * 1024)?;
        let config: Self = super::protocol::decode(&bytes).map_err(|_| invalid("manager_config_invalid"))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> io::Result<()> {
        if let Some(external) = &self.external {
            external.identity.validate().map_err(invalid)?;
            if external.ca_certificate_file.as_ref().is_some_and(|path| !path.is_absolute()) {
                return Err(invalid("manager_ca_path_invalid"));
            }
        }
        if !canonical_id(&self.instance_id)
            || !safe_segment(&self.stable_instance_key)
            || !self.credential_file.is_absolute()
            || !self.spool_directory.is_absolute()
            || !(1_000..=300_000).contains(&self.heartbeat_millis)
            || !(1_000..=300_000).contains(&self.snapshot_millis)
        {
            return Err(invalid("manager_config_invalid"));
        }
        self.endpoint()?;
        Ok(())
    }

    pub fn endpoint(&self) -> io::Result<reqwest::Url> {
        let url = reqwest::Url::parse(&self.rpc_url).map_err(|_| invalid("manager_endpoint_invalid"))?;
        let loopback = url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        let http_allowed = self.external.as_ref().map_or(loopback, |external| external.allow_private_http);
        if !matches!(url.scheme(), "http" | "https")
            || !http_allowed && url.scheme() != "https"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
            || url.host_str().is_none()
        {
            return Err(invalid("manager_endpoint_invalid"));
        }
        Ok(url)
    }

    pub fn token(&self) -> io::Result<Token> {
        let bytes = read_private(&self.credential_file, if self.external.is_some() { 514 } else { 128 })?;
        let value = String::from_utf8(bytes).map_err(|_| invalid("manager_credential_invalid"))?;
        if self.external.is_some() {
            let value = value.trim_end_matches(['\r', '\n']);
            if !(32..=512).contains(&value.len()) || !value.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-') {
                return Err(invalid("manager_credential_invalid"));
            }
            return Ok(Token(value.to_owned()));
        }
        if value.len() != 43 || !value.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-') {
            return Err(invalid("manager_credential_invalid"));
        }
        // A 32-byte base64url value has four significant bits and two zero padding bits in its final sextet.
        if !matches!(
            value.as_bytes()[42],
            b'A' | b'E' | b'I' | b'M' | b'Q' | b'U' | b'Y' | b'c' | b'g' | b'k' | b'o' | b's' | b'w' | b'0' | b'4' | b'8'
        ) {
            return Err(invalid("manager_credential_invalid"));
        }
        Ok(Token(value))
    }

    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.heartbeat_millis)
    }
}

pub(super) fn invalid(code: &'static str) -> io::Error {
    io::Error::other(FailureCode(code))
}
#[derive(Debug)]
pub(crate) struct FailureCode(&'static str);
impl FailureCode {
    pub(crate) fn code(&self) -> &'static str {
        self.0
    }
}
impl std::fmt::Display for FailureCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for FailureCode {}
pub(super) fn safe_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value != "."
        && value != ".."
        && value.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
}
pub(super) fn canonical_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, c)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                c == b'-'
            } else {
                c.is_ascii_digit() || (b'a'..=b'f').contains(&c)
            }
        })
}

#[cfg(unix)]
pub(super) fn read_private(path: &Path, maximum: u64) -> io::Result<Vec<u8>> {
    use std::os::unix::fs::MetadataExt;
    let file = open_no_follow(path)?;
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.nlink() != 1
        || meta.uid() != rustix::process::geteuid().as_raw()
        || meta.mode() & 0o7777 != 0o600
        || meta.len() > maximum
    {
        return Err(invalid("manager_private_file_invalid"));
    }
    let parent = open_no_follow(path.parent().ok_or_else(|| invalid("manager_private_directory_invalid"))?)?;
    let meta = parent.metadata()?;
    if !meta.is_dir() || meta.uid() != rustix::process::geteuid().as_raw() || meta.mode() & 0o7777 != 0o700 {
        return Err(invalid("manager_private_directory_invalid"));
    }
    let mut bytes = Vec::new();
    file.take(maximum + 1).read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() as u64 > maximum {
        return Err(invalid("manager_private_file_size"));
    }
    Ok(bytes)
}

#[cfg(unix)]
pub(super) fn open_no_follow(path: &Path) -> io::Result<File> {
    use rustix::fs::{Mode, OFlags, openat};
    use std::path::Component;
    if !path.is_absolute() {
        return Err(invalid("manager_absolute_path_required"));
    }
    let mut directory = File::open("/")?;
    for component in path.components() {
        match component {
            Component::RootDir => (),
            Component::Normal(segment) => {
                directory =
                    openat(&directory, segment, OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK, Mode::empty())?
                        .into()
            }
            _ => return Err(invalid("manager_path_invalid")),
        }
    }
    Ok(directory)
}

#[cfg(not(unix))]
pub(super) fn read_private(_path: &Path, _maximum: u64) -> io::Result<Vec<u8>> {
    Err(invalid("manager_integration_requires_linux"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(url: &str) -> Config {
        Config {
            external: None,
            instance_id: "11111111-1111-4111-8111-111111111111".into(),
            stable_instance_key: "primary".into(),
            rpc_url: url.into(),
            credential_file: "/run/unused".into(),
            spool_directory: "/run/spool".into(),
            heartbeat_millis: 30_000,
            snapshot_millis: 10_000,
        }
    }
    #[test]
    fn insecure_or_credential_bearing_endpoints_are_rejected() {
        for url in [
            "http://example.com/",
            "https://user:secret@example.com/",
            "https://example.com/?token=x",
            "https://example.com/rpc/v1/",
            "file:///tmp/secret",
        ] {
            assert!(config(url).endpoint().is_err());
        }
        for url in ["http://127.0.0.1:8088/", "http://[::1]:8088/", "https://example.com/"] {
            assert!(config(url).endpoint().is_ok());
        }
    }
    #[test]
    fn external_https_default_and_explicit_http_do_not_change_managed_policy() {
        let mut c = config("http://127.0.0.1/");
        c.external = Some(ExternalConnection {
            identity: super::super::protocol::ExternalIdentity {
                identity_id: "external".into(),
                run_generation: 1,
                purpose: "live".into(),
            },
            allow_private_http: false,
            ca_certificate_file: None,
        });
        assert!(c.endpoint().is_err());
        c.rpc_url = "https://man-agent:8443/".into();
        assert!(c.validate().is_ok());
        c.rpc_url = "http://man-agent:8088/".into();
        assert!(c.endpoint().is_err());
        c.external.as_mut().unwrap().allow_private_http = true;
        assert!(c.endpoint().is_ok());
        c.external = None;
        assert!(c.endpoint().is_err());
        // External identity is not accepted from a Managed launch file.
        assert!(super::super::protocol::decode::<Config>(br#"{"instanceId":"11111111-1111-4111-8111-111111111111","stableInstanceKey":"primary","rpcUrl":"https://node/","credentialFile":"/run/token","spoolDirectory":"/run/spool","external":{"identityId":"bypass"}}"#).is_err());
    }
    #[cfg(unix)]
    #[test]
    fn credentials_are_exact_owned_private_files_not_links() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "A".repeat(43)).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut c = config("http://127.0.0.1/");
        c.credential_file = path.clone();
        assert!(c.token().is_ok());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(c.token().is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let link = dir.path().join("link");
        symlink(&path, &link).unwrap();
        c.credential_file = link;
        assert!(c.token().is_err());
        let link_dir = dir.path().join("linked-directory");
        symlink(dir.path(), &link_dir).unwrap();
        c.credential_file = link_dir.join("token");
        assert!(c.token().is_err());
    }
}
