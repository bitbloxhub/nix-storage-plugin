# cargo-llvm-cov docs: set `LLVM_COV`/`LLVM_PROFDATA` when `llvm-tools-preview` is not in default toolchain
# fenix docs: nightly `default` may omit `llvm-tools-preview`, but `complete` exposes it as a separate component
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

    hegel = {
      url = "github:hegeldev/hegel-core?ref=v0.4.1&dir=nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-compat.follows = "";
    };
  };

  perSystem =
    {
      pkgs,
      inputs',
      self',
      ...
    }:
    let
      cargoNix = import ../Cargo.nix;

      rustToolchain = inputs'.fenix.packages.default.toolchain;

      llvmTools = inputs'.fenix.packages.complete.llvm-tools-preview;

      llvmToolsBin = "${llvmTools}/lib/rustlib/${pkgs.stdenv.hostPlatform.rust.rustcTarget}/bin";

      cargoWorkspace = pkgs.callPackage cargoNix {
        buildRustCrateForPkgs =
          pkgs:
          with pkgs;
          buildRustCrate.override {
            rustc = rustToolchain;
            cargo = rustToolchain;
          };
      };

      patchedSkopeo = self'.packages.skopeo;
    in
    {
      make-shells.default = {
        packages = [
          inputs'.fenix.packages.default.toolchain
          pkgs.rust-analyzer
          inputs'.crate2nix.packages.default
          pkgs.cargo-nextest
          pkgs.cargo-llvm-cov
          llvmTools
          pkgs.lcov
        ];
        env.HEGEL_SERVER_COMMAND = pkgs.lib.getExe inputs'.hegel.packages.default;
        env.LLVM_COV = "${llvmToolsBin}/llvm-cov";
        env.LLVM_PROFDATA = "${llvmToolsBin}/llvm-profdata";
      };

      packages.default = cargoWorkspace.rootCrate.build.overrideAttrs (old: {
        nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ pkgs.makeWrapper ];

        postFixup = (old.postFixup or "") + ''
          for program in "$out"/bin/*; do
            [ -f "$program" ] || continue
            wrapProgram "$program" \
              --prefix PATH : ${pkgs.lib.makeBinPath [ patchedSkopeo ]}
          done
        '';
      });

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
