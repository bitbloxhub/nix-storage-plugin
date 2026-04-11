{
  self,
  ...
}:
{
  flake.nixosModules.default =
    {
      config,
      lib,
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
      registryLocation = "${cfg.bindAddress}:${toString cfg.port}";
      registryDropIn = registriesToml.generate "90-nix-storage-plugin.conf" {
        registry = [
          {
            prefix = "nix:0";
            location = registryLocation;
            insecure = true;
          }
          {
            prefix = "flake-github:0";
            location = "${registryLocation}/flake-github";
            insecure = true;
          }
          {
            prefix = "flake-tarball-https:0";
            location = "${registryLocation}/flake-tarball-https";
            insecure = true;
          }
          {
            prefix = "flake-tarball-http:0";
            location = "${registryLocation}/flake-tarball-http";
            insecure = true;
          }
          {
            prefix = "flake-git-https:0";
            location = "${registryLocation}/flake-git-https";
            insecure = true;
          }
          {
            prefix = "flake-git-http:0";
            location = "${registryLocation}/flake-git-http";
            insecure = true;
          }
          {
            prefix = "flake-git-ssh:0";
            location = "${registryLocation}/flake-git-ssh";
            insecure = true;
          }
        ];
      };
    in
    {
      options.services.nix-storage-plugin = {
        enable = mkEnableOption "nix-storage-plugin services";

        package = mkOption {
          type = types.package;
          default = defaultPackage;
          description = "Package to use for nix-storage-plugin";
        };

        mountPath = mkOption {
          type = types.str;
          default = "/run/nix-storage-plugin/layer-store";
          description = "Mount path for Additional Layer Store";
        };

        bindAddress = mkOption {
          type = types.str;
          default = "127.0.0.1";
          description = "Address for serve-image to bind to";
        };

        port = mkOption {
          type = types.port;
          default = 45123;
          description = "Port for serve-image registry adapter";
        };

        manageStorageConfig = mkOption {
          type = types.bool;
          default = true;
          description = "Whether to add the Additional Layer Store to containers storage.conf";
        };

        manageRegistryAlias = mkOption {
          type = types.bool;
          default = true;
          description = "Whether to add the nix:0 and flake-* registry alias drop-ins for containers registries.conf";
        };
      };

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

        virtualisation.containers = {
          enable = true;
          storage.settings = mkIf cfg.manageStorageConfig {
            storage.options.additionallayerstores = [
              "${cfg.mountPath}:ref"
            ];
          };
        };

        environment.etc = mkIf cfg.manageRegistryAlias {
          "containers/registries.conf.d/90-nix-storage-plugin.conf".source = registryDropIn;
        };

        systemd.services = {
          nix-storage-plugin-als = {
            description = "nix-storage-plugin Additional Layer Store";
            wantedBy = [ "multi-user.target" ];
            before = [ "crio.service" ];
            serviceConfig = {
              Type = "simple";
              ExecStart = "${cfg.package}/bin/nix-storage-plugin mount-store --mount-path ${cfg.mountPath}";
              Restart = "on-failure";
              RestartSec = 1;
            };
          };

          nix-storage-plugin-registry = {
            description = "nix-storage-plugin registry adapter";
            wantedBy = [ "multi-user.target" ];
            serviceConfig = {
              Type = "simple";
              ExecStart = "${cfg.package}/bin/nix-storage-plugin serve-image --bind ${registryLocation}";
              Restart = "on-failure";
              RestartSec = 1;
            };
          };
        };
      };
    };
}
