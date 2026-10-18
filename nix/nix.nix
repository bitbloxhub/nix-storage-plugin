{
  inputs,
  ...
}:
{
  flake-file.inputs = {
    git-hooks-nix = {
      url = "github:cachix/git-hooks.nix";
      inputs = {
        flake-compat.follows = "";
        nixpkgs.follows = "nixpkgs";
      };
    };
    pedantix = {
      url = "github:Swarsel/pedantix";
      inputs = {
        flake-parts.follows = "flake-parts";
        git-hooks-nix.follows = "git-hooks-nix";
        nixpkgs.follows = "nixpkgs";
        treefmt-nix.follows = "treefmt-nix";
      };
    };
  };

  imports = [
    inputs.pedantix.flakeModules.default
  ];

  perSystem =
    {
      pkgs,
      inputs',
      ...
    }:
    {
      make-shells.default.packages = [
        pkgs.nixfmt
        pkgs.deadnix
        pkgs.statix
        inputs'.pedantix.packages.pedantix
      ];

      treefmt.programs = {
        deadnix.enable = true;
        nixfmt.enable = true;
        pedantix = {
          enable = true;
          excludes = [ "flake.nix" ];
        };
        statix.enable = true;
      };
    };
}
