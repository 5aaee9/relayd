{
  description = "relayd Rust package and development shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    flake-utils.url = "github:numtide/flake-utils";

    crane.url = "github:ipetkov/crane";

    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      crane,
      fenix,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        fenixPkgs = fenix.packages.${system};

        toolchain = fenixPkgs.combine [
          (fenixPkgs.latest.withComponents [
            "cargo"
            "clippy"
            "rust-src"
            "rust-std"
            "rustc"
            "rustfmt"
          ])
        ];

        craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;

        src = craneLib.cleanCargoSource ./.;
        cargoArgs = "-p relayd --bin relayd";

        commonArgs = {
          inherit src;
          strictDeps = true;
          cargoExtraArgs = cargoArgs;

          nativeBuildInputs = [
            pkgs.pkg-config
          ];

          buildInputs = [
            pkgs.sqlite
          ];
        };

        netlinkArgs = commonArgs // {
          cargoExtraArgs = "${cargoArgs} --features netlink";
          buildInputs = commonArgs.buildInputs ++ [
            pkgs.libmnl
            pkgs.libnftnl
          ];
          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
            pkgs.libmnl
            pkgs.libnftnl
          ];
        };

        cargoArtifacts = craneLib.buildDepsOnly (
          commonArgs
          // {
            pname = "relayd-deps";
          }
        );

        netlinkCargoArtifacts = craneLib.buildDepsOnly (
          netlinkArgs
          // {
            pname = "relayd-netlink-deps";
          }
        );

        relayd = craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;
            pname = "relayd";
          }
        );

        relayd-netlink = craneLib.buildPackage (
          netlinkArgs
          // {
            cargoArtifacts = netlinkCargoArtifacts;
            nativeBuildInputs = netlinkArgs.nativeBuildInputs ++ [
              pkgs.makeWrapper
            ];
            pname = "relayd-netlink";
            postInstall = ''
              wrapProgram "$out/bin/relayd" \
                --prefix LD_LIBRARY_PATH : "$LD_LIBRARY_PATH"
            '';
          }
        );
      in
      {
        packages = {
          default = relayd;
          inherit relayd relayd-netlink;
        };

        checks = {
          inherit relayd relayd-netlink;

          relayd-clippy = craneLib.cargoClippy (
            commonArgs
            // {
              inherit cargoArtifacts;
              pname = "relayd-clippy";
              cargoClippyExtraArgs = "--all-targets -- --deny warnings";
            }
          );

          relayd-nextest = craneLib.cargoNextest (
            commonArgs
            // {
              inherit cargoArtifacts;
              pname = "relayd-nextest";
              cargoNextestExtraArgs = "--workspace";
            }
          );

          relayd-fmt = craneLib.cargoFmt {
            inherit src;
            pname = "relayd-fmt";
          };
        };

        devShells.default = craneLib.devShell {
          checks = self.checks.${system};

          packages = with pkgs; [
            cargo-nextest
            fenixPkgs.rust-analyzer
            libmnl
            libnftnl
            pkg-config
            sqlite
            toolchain
          ];
        };

        formatter = pkgs.nixfmt;
      }
    );
}
