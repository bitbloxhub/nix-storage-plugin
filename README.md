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
podman run flake-github:0/pdtpartners/nix--x2d--snapshotter--x23--image--x2d--redis--x57--ith--x53--hell
podman run flake-github:0/pdtpartners/nix--x2d--snapshotter--x3f--ref--x3d--main--x23--image--x2d--redis--x57--ith--x53--hell
```

## Building images

Use [`nix-snapshotter`](https://github.com/pdtpartners/nix-snapshotter) for image creation.

Flake protocol aliases are exposed through the registry adapter using `containers/registries.conf` prefix rewrites. The `flake-*` prefixes map to the local adapter with protocol-specific repository namespaces, so the adapter can decode and build the matching flake URL on demand before serving the resulting OCI archive. For protocol background and upstream discussion, see [`pdtpartners/nix-snapshotter#177`](https://github.com/pdtpartners/nix-snapshotter/issues/177).

Flake refs are encoded into OCI-compatible repository names before they go through the registry alias path. The exact escaping is an internal implementation detail and may change, so use `encode-flake-ref` instead of hand-writing encoded refs:

```bash
nix run .#default -- encode-flake-ref -- 'github:pdtpartners/nix-snapshotter#image-redisWithShell'
# flake-github:0/pdtpartners/nix--x2d--snapshotter--x23--image--x2d--redis--x57--ith--x53--hell

podman run flake-github:0/pdtpartners/nix--x2d--snapshotter--x23--image--x2d--redis--x57--ith--x53--hell
```

## Testing

Use nextest for local and CI-style runs:

```bash
cargo nextest run
```

To skip tests that require network access (flake builds, podman e2e pulls/runs):

```bash
NO_TEST_NETWORK=1 cargo nextest run
```
