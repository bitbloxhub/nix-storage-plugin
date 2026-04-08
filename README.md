# nix-storage-plugin

Prototype Additional Layer Store for Podman/containers-storage that serves nix-snapshotter-style images, including `nix:0/nix/store/*.tar` OCI archives.

## What it does

- mounts a single FUSE Additional Layer Store at `:ref`
- resolves local images lazily from `containers-storage`
- serves `nix:0/nix/store/*.tar` via a tiny local registry adapter
- materializes layers from the original OCI archive for `nix:0/...` refs
- only exposes nix-backed images through the ALS; non-nix images are skipped

## Current commands

```bash
cargo run -- mount-store --mount-path /tmp/nsp-layer-store
cargo run -- serve-image --bind 127.0.0.1:45123
```

## Podman storage config

Example `/tmp/nsp-storage.conf`:

```toml
[storage]
driver = "overlay"
runroot = "/tmp/nsp-runroot"
graphroot = "/tmp/nsp-graphroot"

[storage.options]
additionallayerstores = ["/tmp/nsp-layer-store:ref"]
```

Use it with:

```bash
CONTAINERS_STORAGE_CONF=/tmp/nsp-storage.conf podman ...
```

## Registry alias config

Example `/tmp/nsp-registries.conf`:

```toml
[[registry]]
prefix = "nix:0"
location = "127.0.0.1:45123"
insecure = true
```

Use it with:

```bash
CONTAINERS_REGISTRIES_CONF=/tmp/nsp-registries.conf podman ...
```

## Example

```bash
CONTAINERS_STORAGE_CONF=/tmp/nsp-storage.conf \
CONTAINERS_REGISTRIES_CONF=/tmp/nsp-registries.conf \
~/podman/bin/podman run --rm -it \
	nix:0/nix/store/<hash>-nix-image-redis.tar
```

## Notes

- root listing in the ALS mount is lazy and only shows refs resolved during the current mount lifetime
- `nix:0` is just a registry alias, not a custom image transport
- the local registry adapter is intended for arbitrary `/nix/store/*.tar` OCI archives

For implementation constraints and current engineering direction, see `AGENTS.md`.
