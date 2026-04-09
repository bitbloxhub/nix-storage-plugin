# nix-storage-plugin

[`nix-snapshotter`](https://github.com/pdtpartners/nix-snapshotter) but for Podman, CRI-O, and any other `container-libs/storage` based tools.

## Installation

(NixOS module coming soon!)

Clone the `als-first` branch of [my fork of `container-libs`](https://github.com/bitbloxhub/container-libs).

Build Podman/CRI-O/your other tool with the following appended to `go.mod`:
```gomod
replace (
	go.podman.io/image/v5 => <clone of container-libs>/image
	go.podman.io/storage => <clone of container-libs>/storage
)
```

Then start both of the following processes:
```
nix run .#default -- mount-store
nix run .#default -- serve-image
```

`serve-image` runs on port 45123 by default, you may want to change this via the `--bind` flag if you have multiple users.

Add the following to `~/.config/containers/storage.conf` (`/etc/containers/storage.conf` if system-wide), matching whatever path you pass to `mount-store`:

Rootless:
```toml
additionallayerstores = ["/run/user/1000/nix-storage-plugin/layer-store:ref"]
```

Rootful or system-service:
```toml
additionallayerstores = ["/run/nix-storage-plugin/layer-store:ref"]
```

Add the following to `~/.config/containers/registries.conf` (`/etc/containers/registries.conf` if system-wide):
```toml
[[registry]]
prefix = "nix:0"
location = "127.0.0.1:45123" # Or whatever custom port you used
insecure = true
```

Then you can run whatever `nix-snapshotter` images you want!

## Building images

Just use [`nix-snapshotter`](https://github.pdtpartners/nix-snapshotter) for this.
