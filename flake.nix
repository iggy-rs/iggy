{
  description = "Iggy. Persistent message streaming platform written in Rust";

  inputs = {
    dream2nix.url = "github:nix-community/dream2nix";
    nixpkgs.follows = "dream2nix/nixpkgs";
  };

  outputs = {
    self,
    dream2nix,
    nixpkgs,
    ...
  }: let
    supportedSystems = ["x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin"];
    forEachSupportedSystem = f:
      nixpkgs.lib.genAttrs supportedSystems (supportedSystem:
        f {
          system = supportedSystem;
          pkgs = dream2nix.inputs.nixpkgs.legacyPackages.${supportedSystem};
        });
  in {
    packages = forEachSupportedSystem ({pkgs, ...}: rec {
      sdk = dream2nix.lib.evalModules {
        packageSets.nixpkgs = pkgs;
        modules = [
          ./sdk.nix
          {
            paths.projectRoot = ./.;
            paths.projectRootFile = "flake.nix";
            paths.package = ./.;
          }
        ];
      };
      cli = dream2nix.lib.evalModules {
        packageSets.nixpkgs = pkgs;
        modules = [
          ./cli.nix
          {
            paths.projectRoot = ./.;
            paths.projectRootFile = "flake.nix";
            paths.package = ./.;
          }
        ];
      };
      server = dream2nix.lib.evalModules {
        packageSets.nixpkgs = pkgs;
        modules = [
          ./server.nix
          {
            paths.projectRoot = ./.;
            paths.projectRootFile = "flake.nix";
            paths.package = ./.;
          }
        ];
      };
      bench = dream2nix.lib.evalModules {
        packageSets.nixpkgs = pkgs;
        modules = [
          ./bench.nix
          {
            paths.projectRoot = ./.;
            paths.projectRootFile = "flake.nix";
            paths.package = ./.;
          }
        ];
      };
      tools = dream2nix.lib.evalModules {
        packageSets.nixpkgs = pkgs;
        modules = [
          ./tools.nix
          {
            paths.projectRoot = ./.;
            paths.projectRootFile = "flake.nix";
            paths.package = ./.;
          }
        ];
      };
      default = server;
    });

    formatter = forEachSupportedSystem ({pkgs, ...}: pkgs.alejandra);

    devShells = forEachSupportedSystem ({
      system,
      pkgs,
      ...
    }: {
      default = pkgs.mkShell {
        inputsFrom = [
          self.packages.${system}.default.devShell
        ];

        packages = [];
      };
    });

    overlay = final: prev: {
      iggy-pkgs = self.packages.${prev.system};
    };
  };
}
