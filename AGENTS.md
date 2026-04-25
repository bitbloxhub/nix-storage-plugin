# AGENTS.md

## What this repo is trying to do

Make Podman and other `containers/storage` consumers run nix-snapshotter-compatible images, including `nix:0/...`, through a layer-digest-keyed Additional Layer Store using the `bitbloxhub/container-libs` `als-first` branch.

## Current reality

- the ALS is one long-lived dynamic FUSE mount with lazy on-demand resolution
- `nix:0/nix/store/*.tar` resolves from `oci-archive:/nix/store/*.tar` and uses the archive as the source of truth
- local images resolve from `containers-storage` metadata plus `skopeo` inspection/export helpers
- the ALS intentionally skips non-nix images
- this project now depends on the `https://github.com/bitbloxhub/container-libs/tree/als-first` branch of container-libs
- that means patching/rebuilding Podman, CRI-O, and any other consumers that vendor or depend on the relevant container-libs bits
- no `mount_program` unless compatibility testing proves it is necessary

## Why the extra `container-libs` branch exists

The `bitbloxhub/container-libs` `als-first` branch exists because upstream behavior/issues still block the exact Additional Layer Store ordering/semantics this project needs.

Relevant upstream issues:

- https://github.com/containers/container-libs/issues/747
- https://github.com/containers/container-libs/issues/119
- https://github.com/containers/container-libs/issues/109

When working in this repo, assume those upstream issues are the reason we currently need the branch and downstream consumer patching. If any of those issues land upstream, reevaluate whether the custom branch and consumer patches are still necessary.

## Invariants to keep

- keep compatibility with `containers/storage` Additional Layer Store expectations
- support both `nix:0/...` refs and normal registry refs
- prefer the nix closure annotation path over legacy per-store-path annotations
- keep `nix:0` as a registry alias, not a custom transport
- use the `bitbloxhub/container-libs` `als-first` branch consistently across Podman, CRI-O, and any other affected consumers
- do not assume upstream Podman/CRI-O/container-libs behavior matches this branch without checking

## Important storage assumptions

With `additionallayerstores = ["/path:ref"]`, storage expects paths shaped like:

```text
<mountpoint>/<base64(imageref)>/<layer-key>/
- diff
- info
- blob
- use    # optional
```

The important bit is that `diff`, `info`, and `blob` must exist for a valid external layer entry.

## Current manual workflow

Start the ALS:

```bash
cargo run -- mount-store --mount-path /tmp/nsp-layer-store
```

Start the local registry adapter:

```bash
cargo run -- serve-image --bind 127.0.0.1:45123
```

Example Podman runs:

```bash
podman run ghcr.io/pdtpartners/redis-shell:latest
podman run nix:0/nix/store/<hash>-nix-image-redis.tar
```

## Next likely work

1. add smoke/integration tests for archive-backed nix images, `redis-shell`, and non-nix skip behavior
2. improve malformed `/nix/store/*.tar` error messages
3. keep the `als-first` branch integration working across Podman, CRI-O, and related consumers
4. consider registry cache bounds/eviction if this becomes long-lived

## Success looks like

- Podman runs nix-snapshotter-compatible images
- CRI-O and other relevant `containers/storage` consumers can use the same ALS behavior via the `als-first` branch
- `nix:0/...` works with one-time config only
- nix-backed images resolve lazily through the ALS
- non-nix images are skipped cleanly

## Testing

- use Hegel for property-based tests in pure logic-heavy code when properties are clear
- prefer `#[hegel::test(derandomize = true)]` so generated cases are deterministic across runs without hard-coding arbitrary numeric seeds
- keep Hegel tests in existing module test blocks instead of separate dedicated hegel test files
- when inspecting generated examples, temporarily raise Hegel verbosity on the test, e.g. `#[hegel::test(derandomize = true, verbosity = hegel::Verbosity::Debug)]`
- Hegel always shows minimized counterexamples on failure; use higher verbosity only when actively debugging generated cases
- prefer `cargo nextest run` over `cargo test` for normal test runs once `cargo-nextest` is available in the dev shell
- install `cargo-llvm-cov` in the Nix dev shell and prefer coverage-backed test runs by default
- prefer `cargo llvm-cov nextest` over plain `cargo nextest run` when validating Rust changes so coverage stays current
- common coverage outputs: `cargo llvm-cov nextest --html` for local browsing and `cargo llvm-cov nextest --lcov --output-path lcov.info` for CI/reporting
- use `cargo test` only when you specifically need raw libtest behavior or a tool does not integrate cleanly with nextest
