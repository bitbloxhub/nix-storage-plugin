# nix-storage-plugin

[`nix-snapshotter`](https://github.com/pdtpartners/nix-snapshotter) but for Podman, CRI-O, and any other `container-libs/storage` based tools.

## NixOS

Use overlay plus module:

```nix
{
  nixpkgs.overlays = [ nix-storage-plugin.overlays.default ];

  imports = [ nix-storage-plugin.nixosModules.default ];

  services.nix-storage-plugin.enable = true;
}
```

Main options:

```nix
services.nix-storage-plugin = {
  mountPath = "/run/nix-storage-plugin/layer-store";
  bindAddress = "127.0.0.1";
  port = 45123;
  manageStorageConfig = true;
  manageRegistryAlias = true;
};
```

## Manual patching builds

If not using overlay, patch your container consumer against [`bitbloxhub/container-libs`](https://github.com/bitbloxhub/container-libs) `als-first`:

```gomod
replace (
	go.podman.io/image/v5 => <clone of container-libs>/image
	go.podman.io/storage => <clone of container-libs>/storage
)
```

Run services with:

```bash
nix run .#default -- mount-store
nix run .#default -- serve-image
```

Then you can run `nix-snapshotter`-compatible images, including:

```bash
podman run ghcr.io/pdtpartners/redis-shell:latest
podman run nix:0/nix/store/<hash>-nix-image-redis.tar
```

## Building images

Use [`nix-snapshotter`](https://github.com/pdtpartners/nix-snapshotter) for image creation.
