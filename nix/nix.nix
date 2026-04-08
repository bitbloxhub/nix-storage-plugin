{
  perSystem =
    {
      pkgs,
      ...
    }:
    {
      make-shells.default = {
        packages = [
          pkgs.nixfmt
          pkgs.deadnix
          pkgs.statix
        ];
      };

      treefmt = {
        programs.nixfmt.enable = true;
        programs.deadnix.enable = true;
        programs.statix.enable = true;
      };
    };
}
