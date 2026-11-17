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
              "-X main.BuildTime=${self.lastModifiedDate or "unknown"}"
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
