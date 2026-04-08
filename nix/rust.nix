_: {
  flake-file.inputs = {
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    crate2nix = {
      url = "github:nix-community/crate2nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-compat.follows = "";
      inputs.flake-parts.follows = "flake-parts";
      inputs.crate2nix_stable.follows = "crate2nix";
      inputs.cachix.follows = "";
    };
  };

  perSystem =
    {
      pkgs,
      inputs',
      ...
    }:
    let
      cargoNix = import ../Cargo.nix;

      cargoWorkspace = pkgs.callPackage cargoNix {
        buildRustCrateForPkgs =
          pkgs:
          with pkgs;
          buildRustCrate.override {
            rustc = inputs'.fenix.packages.default.toolchain;
            cargo = inputs'.fenix.packages.default.toolchain;
          };
      };
    in
    {
      make-shells.default = {
        packages = [
          inputs'.fenix.packages.default.toolchain
          pkgs.rust-analyzer
          inputs'.crate2nix.packages.default
        ];
      };

      packages.default = cargoWorkspace.rootCrate.build;

      treefmt = {
        programs.rustfmt = {
          enable = true;
          package = inputs'.fenix.packages.default.rustfmt;
        };
        settings.global.excludes = [
          "Cargo.nix"
        ];
      };
    };
}
