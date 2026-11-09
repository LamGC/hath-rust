# hath-rust
[![Build](../../actions/workflows/build.yml/badge.svg)](../../actions/workflows/build.yml)

The unofficial Hentai@Home client written in Rust.

## hath-manager integration (development)

This fork contains an optional `manager-integration` feature, enabled in default builds.
It does not activate the integration unless explicitly configured. The protocol baseline
is 1.0 and the native peer identity is distinct from a JavaAgent/JVM identity.

Linux managed startup is wired and reports `launchSupported: true`. The Manager must
pin the local ELF's SHA-256 and provide a private `--manager-config` file; missing or
invalid credentials never trigger interactive setup. Client readiness requires upstream
startup confirmation, not just Manager registration or a listening TLS socket.

The actual executable has isolated startup/Gallery tests, including outbox degradation,
certificate/startup failures, login cancellation, and standalone Noop operation. The
Manager-side driver has mock-validated lifecycle/crash recovery; historical import also
uses the actual Rust offline helper. An unavailable durable outbox pauses Gallery while
an independent reporter sends degraded health and cache service can continue. Competing
writers are fatal. Repair an initialization failure and restart the client to retry it.

The process owner joins Gallery writers and background engines on shutdown and startup
errors. Logger/RPC destruction does not synchronously wait for async work; explicit RPC
shutdown bounds upstream notification to ten seconds. Without explicit Manager configuration,
standalone operation creates no Manager connections or metadata and retains its original
download layout. Non-Linux managed startup remains unsupported.

Optional telemetry now samples native counters without scanning the cache or keeping
per-request history. RPC, cache, authenticated servercmd and dispatch use fixed operation
names; unknown actions share an `other` bucket. Missing metrics are explicitly unsupported,
not zero. Telemetry failures retry independently of reliable delivery when the Session is
still valid. HTTP Body accounting distinguishes completion, errors and cancellation;
HEAD/304/Range cannot become full-image success. Proxy upstream validation, downstream
views and atomic cache import are separate observations. Managed diagnostics are
sanitized before formatting, with bounded queues, lines and rotating files.

```sh
cargo test --locked
cargo test --locked --no-default-features
cargo run --locked -- --manager-describe --json
```

The descriptor exits before credentials, logging, runtime directories or EH networking.
The engine is Linux-first; it uses private owner-only files and a bounded disk outbox.
Tests require OpenSSL to generate throwaway TLS fixtures. The parent workspace has also
validated actual Rust → Manager → synthetic asynchronous Provider → qBittorrent → BT
readback in both ZIP Store and directory layouts. A short 512MB no-swap experiment at
about 96 QPS had no OOM; it does not establish million-file, 1000-QPS or production EH
capacity. Native automatic updates, server-side bandwidth semantics and atomic cache
state crash recovery remain separate production work.

## Install
Native fork binaries are published on [GitHub Releases](https://github.com/LamGC/hath-rust/releases).
Choose the asset matching your platform and verify it with `SHA256SUMS`.
`release-manifest.json` records the release version, source commit, platform assets and hashes.
Checksums are not a publisher signature; a signed source tag does not sign the compiled binaries.
CI creates a **draft** for administrator review and manual publication, never a public release automatically.
This fork no longer publishes Docker images; Dockerfiles remain for upstream maintenance.
See [CHANGELOG.md](CHANGELOG.md) for fork changes, upstream synchronization and known limitations.

Read the [Wiki](https://github.com/james58899/hath-rust/wiki/Install)

## Features
### New
Features not included in the official.
* Lower memory usage
* Parallel async cache scan
* Seamless certificate update
* Using ChaCha20 on hardware without AES acceleration
* Send filename to browser[^filename]
* Monitoring endpoints ([wiki](https://github.com/james58899/hath-rust/wiki/Monitoring))
* Strict SNI checking (Default off, may reduce quality)
* Post-quantum cryptography[^pqc]
* HTTP/3 (Experimental)
* Range request and ETag support[^range]

### Works
Features that are included in the official and are working.
* Cache and Proxy
* Gallery downloader
* Speed test
* Cache size management
* Logging
* Disk space check
* Download cache files through proxy
* Flood control

### No planned
* HTTP/2[^h2]
* Bandwidth limit

## Platform support
Please refer to the rust website for the platform name: https://doc.rust-lang.org/stable/rustc/platform-support.html

### Tier 1
Main supported platforms.  
Tested in real environments before release.

|          Platform         |
| ------------------------- |
| x86_64-unknown-linux-gnu  |
| x86_64-unknown-linux-musl |

### Tier 2
Secondary supported platforms.  
Due to the lack of hardware or real environment, it was not tested before release, relying on users to report bugs.

|            Platform            |
| ------------------------------ |
| aarch64-unknown-linux-gnu      |
| aarch64-unknown-linux-musl     |
| armv7-unknown-linux-gnueabihf  |
| armv7-unknown-linux-musleabihf |
| x86_64-pc-windows-msvc         |
| i686-pc-windows-msvc           |
| aarch64-pc-windows-msvc        |
| x86_64-apple-darwin            |
| aarch64-apple-darwin           |

### Tier 3
Experimental platform.  
Not guaranteed to work, may break at any time.

|         Platform        |
| ----------------------- |
| aarch64-linux-android   |
| armv7-linux-androideabi |
| i686-linux-android      |
| x86_64-linux-android    |


[^h2]: Multiplexing is useless for H@H, and a large number of connections will take up more system resources.
[^filename]: If the filename is not sent, some browsers may download using the wrong name.
[^pqc]: Support X25519MLKEM768 key agreement
[^range]: Allows the browser to resume interrupted image downloads.
