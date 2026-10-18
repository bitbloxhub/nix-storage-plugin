# cargo-llvm-cov docs: set `LLVM_COV`/`LLVM_PROFDATA` when `llvm-tools-preview` is not in default toolchain
# fenix docs: nightly `default` may omit `llvm-tools-preview`, but `complete` exposes it as a separate component
{
  flake-file.inputs = {
    crate2nix = {
      url = "github:nix-community/crate2nix";
      inputs = {
        cachix.follows = "";
        flake-compat.follows = "";
        flake-parts.follows = "flake-parts";
        nixpkgs.follows = "nixpkgs";
      };
    };
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    hegel = {
      url = "github:hegeldev/hegel-core?ref=v0.9.1&dir=nix";
      inputs = {
        flake-compat.follows = "";
        nixpkgs.follows = "nixpkgs";
      };
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
      cargoWorkspace = pkgs.callPackage cargoNix {
        buildRustCrateForPkgs =
          pkgs:
          with pkgs;
          buildRustCrate.override {
            cargo = rustToolchain;
            rustc = rustToolchain;
          };
      };
      llvmTools = inputs'.fenix.packages.complete.llvm-tools-preview;
      llvmToolsBin = "${llvmTools}/lib/rustlib/${pkgs.stdenv.hostPlatform.rust.rustcTarget}/bin";
      patchedSkopeo = self'.packages.skopeo;
      rustToolchain = inputs'.fenix.packages.default.toolchain;
    in
    {
      make-shells.default = {
        env = {
          HEGEL_SERVER_COMMAND = pkgs.lib.getExe inputs'.hegel.packages.default;
          LLVM_COV = "${llvmToolsBin}/llvm-cov";
          LLVM_PROFDATA = "${llvmToolsBin}/llvm-profdata";
        };
        packages = [
          inputs'.fenix.packages.default.toolchain
          pkgs.rust-analyzer
          inputs'.crate2nix.packages.default
          pkgs.cargo-nextest
          pkgs.cargo-llvm-cov
          llvmTools
          pkgs.lcov
        ];
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

  flatFlake.config.allowed = [
    # Inputs used internally by the tools declared above.
    [
      "crate2nix"
      "devshell"
    ]
    [
      "crate2nix"
      "nix-test-runner"
    ]
    [
      "crate2nix"
      "pre-commit-hooks"
    ]
    [
      "fenix"
      "rust-analyzer-src"
    ]
    [
      "hegel"
      "pyproject-build-systems"
    ]
    [
      "hegel"
      "pyproject-nix"
    ]
    [
      "hegel"
      "uv2nix"
    ]
  ];
}
