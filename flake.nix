{
  description = "A Matrix bridge for traQ";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      # self.lastModifiedDate is YYYYMMDDhhmmss in UTC; main.BuildTime has to be RFC 3339 to be parsed.
      # It comes from the commit, so the build stays reproducible.
      buildTime =
        let
          d = self.lastModifiedDate or null;
          part = start: len: builtins.substring start len d;
        in
        if d == null then
          "unknown"
        else
          "${part 0 4}-${part 4 2}-${part 6 2}T${part 8 2}:${part 10 2}:${part 12 2}Z";
      # olm is marked insecure in nixpkgs. It is only used for end-to-bridge encryption.
      pkgsFor =
        system:
        import nixpkgs {
          inherit system;
          config.permittedInsecurePackages = [ "olm-3.2.16" ];
        };
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = pkgsFor system;
        in
        {
          default = pkgs.buildGoModule rec {
            pname = "traq-matrix-bridge";
            version = "0.1.0";
            src = self;
            subPackages = [ "cmd/traq-matrix-bridge" ];
            vendorHash = "sha256-38q4HK6mkCXMpnaZvmbevdUEKIX7jveGEzarhXDD4ZA=";
            buildInputs = [ pkgs.olm ];
            ldflags = [
              "-s"
              "-w"
              "-X main.Tag=v${version}"
              "-X main.Commit=${self.shortRev or self.dirtyShortRev or "unknown"}"
              "-X main.BuildTime=${buildTime}"
            ];
            meta = {
              description = "A Matrix bridge for traQ";
              homepage = "https://github.com/kisepichu/traq-matrix-bridge";
              license = pkgs.lib.licenses.mit;
              mainProgram = "traq-matrix-bridge";
            };
          };
        }
      );

      devShells = forAllSystems (
        system:
        let
          pkgs = pkgsFor system;
        in
        {
          default = pkgs.mkShell {
            packages = [
              pkgs.go
              pkgs.gopls
              pkgs.olm
              pkgs.pkg-config
            ];
          };
        }
      );

      checks = forAllSystems (system: {
        package = self.packages.${system}.default;
      });
    };
}
