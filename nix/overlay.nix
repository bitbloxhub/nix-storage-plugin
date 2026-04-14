_:
let
  alsFirstPatchUrl = "https://github.com/bitbloxhub/container-libs/commit/75e979d6aba85c2699ef6e7bf01a214b48025911.patch";

  defaultOverlay =
    final: prev:
    let
      imagePatch = prev.fetchpatch {
        url = alsFirstPatchUrl;
        hash = "sha256-L7Pysl4YbD/ThroJbrk+OqyRl7je1wENJzOkdTK2aLs=";
        relative = "image";
        extraPrefix = "vendor/go.podman.io/image/v5/";
        excludes = [ "*_test.go" ];
      };

      storagePatch = prev.fetchpatch {
        url = alsFirstPatchUrl;
        hash = "sha256-Xw9of20n1fl418V2XmkUkzBn1DlBZSRhoQzMP/wpzsE=";
        relative = "storage";
        extraPrefix = "vendor/go.podman.io/storage/";
        excludes = [ "*_test.go" ];
      };

      patchContainerLibs =
        pkg:
        pkg.overrideAttrs (old: {
          postConfigure = (old.postConfigure or "") + ''
            if [ ! -d vendor ]; then
              echo "expected vendored tree at ./vendor after configurePhase" >&2
              exit 1
            fi

            patch --batch -p1 < ${imagePatch}
            patch --batch -p1 < ${storagePatch}
          '';
        });
    in
    {
      podman = patchContainerLibs prev.podman;

      cri-o-unwrapped = patchContainerLibs prev.cri-o-unwrapped;
      cri-o = prev.cri-o.override {
        inherit (final) cri-o-unwrapped;
      };

      buildah-unwrapped = patchContainerLibs prev.buildah-unwrapped;
      buildah = prev.buildah.override {
        inherit (final) buildah-unwrapped;
      };

      skopeo = patchContainerLibs prev.skopeo;
    };
in
{
  flake.overlays.default = defaultOverlay;

  perSystem =
    {
      pkgs,
      ...
    }:
    let
      patchedPkgs = pkgs.extend defaultOverlay;
    in
    {
      packages = {
        inherit (patchedPkgs) podman;
        inherit (patchedPkgs) cri-o;
        inherit (patchedPkgs) cri-o-unwrapped;
        inherit (patchedPkgs) buildah;
        inherit (patchedPkgs) buildah-unwrapped;
        inherit (patchedPkgs) skopeo;
      };
    };
}
