# 独立容器部署（不接入 Manager）

本示例没有 `--manager-config`，不会连接 hath-manager，也不需要 man-agent。
缓存、身份、下载和日志全部放在挂载的 `/hath`，重建容器不会重新创建身份。

已发布的 `1.19.2` 提供 linux/amd64 和 linux/arm64 标准镜像，可用于本页的独立部署。
正式版本仅在管理员手动发布对应 Release 后发布；开发镜像不冒充正式版本。
`1.20.0` 开始提供 External 接入；发布草稿及正式容器构建完成前，请勿假定该镜像已经可拉取。
External 不包含在 `1.19.2` 中，不要给旧版本配置该模式。

## 1. 数据和身份

默认容器 UID/GID 都为 `10001`。使用其他 UID/GID 时同时调整目录所有权和 Compose 环境变量。

```sh
sudo install -d -o 10001 -g 10001 -m 0700 /srv/hath-rust
sudo install -d -o 10001 -g 10001 -m 0700 /srv/hath-rust/data
sudo install -o 10001 -g 10001 -m 0600 /dev/null /srv/hath-rust/data/client_login
sudoedit /srv/hath-rust/data/client_login
```

在受限文件中写入一行 `ClientID-ClientKey`，不要把真实 Key 放入命令行、Compose 或聊天。
ClientKey 为官方发放的 20 位字母数字串。一个有效客户端身份只运行一个实例；不要让旧客户端同时使用它。
也可以使用客户端交互式初始化，但其现有输入提示可能回显 Key。

## 2. 启动

把 `compose.standalone.yml` 保存到部署目录，设置：

```sh
export HATH_IMAGE='ghcr.io/lamgc/hath-rust:1.19.2'
export HATH_PORT=8080
docker compose -f compose.standalone.yml config --quiet
docker compose -f compose.standalone.yml up -d
docker compose -f compose.standalone.yml logs --tail 100 -f
```

`HATH_PORT` 需为 1024–65535 的可用 TCP 端口，并与官方客户端设置、Oracle 安全列表/NSG
及系统防火墙保持一致。示例内外端口相同。H@H 自己处理 TLS，不需要用 Caddy/Nginx 反向代理图片端口。
确认客户端已被官方控制服务器接纳后再长期运行；本项目的离线测试不代替官方注册/网络检查。

停止使用 `docker compose -f compose.standalone.yml down`，不删除数据目录。
`1.20.0` 的 External 配置与独立 man-agent 部署示例在配套 hath-manager 仓库的 `deploy/containers/`；
不要为了接入 Manager 把同一数据目录交给另一个 Managed 实例启动。

## 可选：Traefik TCP 透传与 PROXY protocol v2

需要包含 PROXY protocol v2 支持的客户端构建；这项能力不依赖 Manager。不开启时原有直连方式不变。
启用时在现有参数后加 `--proxy-protocol-v2 --trusted-proxy <Traefik 后端 IP>`。
`--trusted-proxy` 可重复，接受精确 IP 或网络地址形式的 CIDR（如 `192.0.2.0/24`）；带主机位的 CIDR 会被拒绝。
优先用 Traefik 在专用后端网络上的精确 IP，不信任公共网段或整个共享网络。

- 可信对端必须先发送 PROXY v2 的 TCP/IPv4 或 TCP/IPv6 头，再发送 TLS。缺失、无效或超时都会断开。
- 其他对端直接进入原有 TLS 流程，不解析 PROXY 头。合法 TLS 直连不变，带 PROXY 头的直连不会通过握手。
- TLS 握手、证书和现有 IP 校验继续由 hath-rust 处理。此配置只处理 TCP，不能同时使用 `--enable-h3`；避免向访客通告 Traefik 并未转发的 UDP 端点。

可参考 [Traefik v3 动态配置示例](traefik.hath-tcp.example.yml)，将 `hath:11443` 和 entrypoint 换成实际地址与名称。
示例在 `websecure` 上透传 `hath.network` 及子域名的 SNI；不要在共用入口放接管所有 TLS 的兜底匹配。
使用独立后端网络，让 Traefik 能访问 hath-rust 的 TCP 端口，并确保 hath-rust 实际看到的 TCP 对端 IP 等于可信配置。
不要让普通直连经 SNAT 后看起来也来自这个可信地址；否则 hath-rust 无法区分它们和代理流量。

先用测试入口验证图片、测速和控制请求。此前保持 EH 登记端口及直连配置不变。
只有确认实际 H@H 请求都带有可匹配的 SNI 后，才共用公网 `443` 并同步调整 EH 登记的端口；否则继续使用独立端口。

## 本地构建

在 fork 仓库根目录执行（应使用干净、已提交的源码）：

```sh
docker build --build-arg HATH_BUILD_COMMIT="$(git rev-parse HEAD)" -t hath-rust:local .
docker run --rm --network none --read-only hath-rust:local --manager-describe --json
```

镜像带集成模块，但是否启用仍由启动参数决定。构建只复制 Rust 源码及 Cargo 清单，不复制运行数据或 `.git`；
源码提交号由构建参数传入。该标签/描述符不是二进制 GPG 签名。

## 发布规则（维护者）

- 默认分支相关改动或在默认分支手动运行 Container workflow，生成 `dev-<完整 SHA>`，并在仍为分支最新提交时更新 `dev`。
- PR 只构建并进行离线启动检查，不推送，不授予软件包写权限。amd64/arm64 分别用原生 runner 构建，不在默认测试中跑长期负载。
- 现有 Release workflow 仍只创建草稿；管理员手动发布 Release 后才触发正式镜像构建，检查签名 tag、固定主密钥指纹、Cargo 版本和更新日志。
- 正式镜像标签为 SemVer（无 `v` 前缀；`+` 构建元数据分隔符在镜像标签中改为 `_`）。预发布不更新 `latest`，稳定版仅在版本严格更新时推进 `latest`。
- 已存在的版本或完整 SHA 标签只允许复用已验证的原摘要，不能覆盖重建。需要修复镜像时使用新提交/新版本。签名源 tag 不等于已对镜像本身签名。
- 两个平台均通过 `--network none --read-only` 的离线版本/描述符检查后才发布多架构标签；不会在 CI 中携带官方 ClientKey 或连接官方控制服务。
- 首次 GHCR 发布后，请在 Package 设置中确认可见性为 Public，或在部署机器配置只读拉取凭据；仓库公开不保证新建 Package 自动公开。应实际匿名拉取检查后再对外宣布可用。
- 第一次可以先交付开发镜像。不要移动已发布的 `v1.19.0` tag 来补 CI；下一正式 Release 必须使用包含此工作流的新版本 tag。
