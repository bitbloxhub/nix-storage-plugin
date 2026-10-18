{
  inputs,
  self,
  ...
}:
{
  flake-file = {
    formatter =
      pkgs:
      pkgs.writeShellApplication {
        name = "flake-file-treefmt";
        runtimeInputs = [
          self.formatter.${pkgs.stdenv.hostPlatform.system}
        ];
        text = ''
          # Match treefmt-nix check: format a writable copy of the complete project.
          input="$1"
          tmpdir=$(mktemp -d)
          project="$tmpdir/project"
          cp -r ${inputs.self} "$project"
          chmod -R a+w "$project"
          cp "$input" "$project/flake.nix"
          (cd "$project" && treefmt --no-cache)
          cp "$project/flake.nix" "$input"
          rm -rf "$tmpdir"
        '';
      };
    inputs = {
      flake-file.url = "github:vic/flake-file";
      flake-parts = {
        url = "github:hercules-ci/flake-parts";
        inputs.nixpkgs-lib.follows = "nixpkgs";
      };
    };
    outputs =
      # nix
      ''
        inputs:
        inputs.flake-parts.lib.mkFlake { inherit inputs; } (
          (inputs.import-tree.filterNot (inputs.nixpkgs.lib.hasSuffix "npins/default.nix")) ./nix
        )
      '';
  };

  imports = [
    inputs.flake-file.flakeModules.default
    inputs.flake-file.flakeModules.import-tree
  ];

  systems = [
    "x86_64-linux"
    "aarch64-linux"
    "aarch64-darwin"
  ];
}
