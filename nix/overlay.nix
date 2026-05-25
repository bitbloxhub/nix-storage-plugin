{ self, ... }:
let
  alsFirstPatchUrl = "https://github.com/bitbloxhub/container-libs/commit/1c9838fc674b59b204b35d218767f1f103d884a7.patch";

  defaultOverlay =
    _final: prev:
    let
      system = prev.stdenv.hostPlatform.system;
    in
    {
      podman = self.packages.${system}.podman;
      cri-o-unwrapped = self.packages.${system}.cri-o-unwrapped;
      cri-o = self.packages.${system}.cri-o;
      buildah-unwrapped = self.packages.${system}.buildah-unwrapped;
      buildah = self.packages.${system}.buildah;
      skopeo = self.packages.${system}.skopeo;
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
      imagePatch = pkgs.fetchpatch {
        url = alsFirstPatchUrl;
        hash = "sha256-L7Pysl4YbD/ThroJbrk+OqyRl7je1wENJzOkdTK2aLs=";
        relative = "image";
        extraPrefix = "vendor/go.podman.io/image/v5/";
        excludes = [ "*_test.go" ];
      };

      storagePatch = pkgs.fetchpatch {
        url = alsFirstPatchUrl;
        hash = "sha256-bpbyLEv4sgEABOBnkLcEnX3DcCLp2BU82jh8LW+M9Fg=";
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

      patchedPkgs = pkgs.extend (
        final: prev: {
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
        }
      );
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
