{
  inputs,
  ...
}:
{
  flake-file.inputs.flat-flake = {
    url = "github:linyinfeng/flat-flake";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  imports = [
    inputs.flat-flake.flakeModules.flatFlake
  ];

  perSystem =
    {
      inputs',
      ...
    }:
    {
      make-shells.default.packages = [
        inputs'.flat-flake.packages.default
      ];
    };
}
