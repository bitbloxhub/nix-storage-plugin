{
  self,
  ...
}:
{
  flake.nixosModules.default =
    {
      lib,
      config,
      pkgs,
      ...
    }:
    let
      inherit (lib)
        mkEnableOption
        mkIf
        mkOption
        types
        ;
      cfg = config.services.nix-storage-plugin;
      defaultPackage = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      registriesToml = pkgs.formats.toml { };
      registryDropIn = registriesToml.generate "90-nix-storage-plugin.conf" {
        registry = [
          {
            insecure = true;
            location = registryLocation;
            prefix = "nix:0";
          }
          {
            insecure = true;
            location = "${registryLocation}/flake-github";
            prefix = "flake-github:0";
          }
          {
            insecure = true;
            location = "${registryLocation}/flake-tarball-https";
            prefix = "flake-tarball-https:0";
          }
          {
            insecure = true;
            location = "${registryLocation}/flake-tarball-http";
            prefix = "flake-tarball-http:0";
          }
          {
            insecure = true;
            location = "${registryLocation}/flake-git-https";
            prefix = "flake-git-https:0";
          }
          {
            insecure = true;
            location = "${registryLocation}/flake-git-http";
            prefix = "flake-git-http:0";
          }
          {
            insecure = true;
            location = "${registryLocation}/flake-git-ssh";
            prefix = "flake-git-ssh:0";
          }
        ];
      };
      registryLocation = "${cfg.bindAddress}:${toString cfg.port}";
    in
    {
      config = mkIf cfg.enable {
        assertions = [
          {
            assertion = lib.hasPrefix "/" cfg.mountPath;
            message = "services.nix-storage-plugin.mountPath must be an absolute path";
          }
          {
            assertion = cfg.bindAddress != "";
            message = "services.nix-storage-plugin.bindAddress must not be empty";
          }
        ];
        environment.etc = mkIf cfg.manageRegistryAlias {
          "containers/registries.conf.d/90-nix-storage-plugin.conf".source = registryDropIn;
        };
        systemd.services = {
          nix-storage-plugin-als = {
            before = [ "crio.service" ];
            description = "nix-storage-plugin Additional Layer Store";
            serviceConfig = {
              ExecStart = "${cfg.package}/bin/nix-storage-plugin mount-store --mount-path ${cfg.mountPath}";
              Restart = "on-failure";
              RestartSec = 1;
              Type = "simple";
            };
            wantedBy = [ "multi-user.target" ];
          };

          nix-storage-plugin-registry = {
            description = "nix-storage-plugin registry adapter";
            serviceConfig = {
              ExecStart = "${cfg.package}/bin/nix-storage-plugin serve-image --bind ${registryLocation}";
              Restart = "on-failure";
              RestartSec = 1;
              Type = "simple";
            };
            wantedBy = [ "multi-user.target" ];
          };
        };
        virtualisation.containers = {
          enable = true;
          storage.settings = mkIf cfg.manageStorageConfig {
            storage.options.additionallayerstores = [
              "${cfg.mountPath}:ref"
            ];
          };
        };
      };
      options.services.nix-storage-plugin = {
        bindAddress = mkOption {
          default = "127.0.0.1";
          description = "Address for serve-image to bind to";
          type = types.str;
        };
        enable = mkEnableOption "nix-storage-plugin services";
        manageRegistryAlias = mkOption {
          default = true;
          description = "Whether to add the nix:0 and flake-* registry alias drop-ins for containers registries.conf";
          type = types.bool;
        };
        manageStorageConfig = mkOption {
          default = true;
          description = "Whether to add the Additional Layer Store to containers storage.conf";
          type = types.bool;
        };
        mountPath = mkOption {
          default = "/run/nix-storage-plugin/layer-store";
          description = "Mount path for Additional Layer Store";
          type = types.str;
        };
        package = mkOption {
          default = defaultPackage;
          description = "Package to use for nix-storage-plugin";
          type = types.package;
        };
        port = mkOption {
          default = 45123;
          description = "Port for serve-image registry adapter";
          type = types.port;
        };
      };
    };
}
