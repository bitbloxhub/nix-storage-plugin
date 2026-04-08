{
  inputs,
  ...
}:
{
  flake-file.inputs = {
    make-shell = {
      url = "github:nicknovitski/make-shell";
      inputs.flake-compat.follows = "";
    };
  };

  imports = [
    inputs.make-shell.flakeModules.default
  ];

  perSystem =
    {
      pkgs,
      ...
    }:
    {
      make-shells.default = {
        name = "nix-storage-plugin";
        packages = [
          pkgs.podman
          pkgs.crun
        ];
      };
    };
}
