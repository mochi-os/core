# Mochi server

Core server for the Mochi project. Written in Go, distributed as native
packages (.deb / .rpm / .msi / .pkg) and as a multi-arch Docker image.

## Building from source

```
git clone git@github.com:mochi-os/core.git
cd core
make             # builds bin/mochi-server and bin/mochictl
```

Requires Go 1.25 or newer. **mochictl** is Linux-only.

## Installing

| Platform | Path |
|----------|------|
| Debian / Ubuntu      | https://docs.mochi-os.org/wikis/yuGtwdxVh/install-deb |
| Fedora / RHEL        | https://docs.mochi-os.org/wikis/yuGtwdxVh/install-rpm |
| macOS                | https://docs.mochi-os.org/wikis/yuGtwdxVh/install-macos |
| Windows              | https://docs.mochi-os.org/wikis/yuGtwdxVh/install-windows |
| Docker               | https://docs.mochi-os.org/wikis/yuGtwdxVh/install-docker |
| From source (this repo) | https://docs.mochi-os.org/wikis/yuGtwdxVh/install-git |

## Layout

| Directory | Contents |
|-----------|----------|
| `server/`        | Mochi server (`mochi-server`) |
| `mochictl/`      | Linux admin/ops CLI (`mochictl`) |
| `common/`        | Shared packages (`ini`, `adminclient`) |
| `build/`         | Per-format packaging (deb / rpm / msi / pkg / docker) |
| `install/`       | Files placed on the host filesystem by deb / rpm |
| `docs/`          | Hand-written man pages |

## Operations

Once installed, the **mochictl** CLI handles backups, status checks, and
lifecycle: see `man mochictl` or
https://docs.mochi-os.org/wikis/yuGtwdxVh/backup-restore.

## License

Copyright Alistair Cunningham 2024-2026.
