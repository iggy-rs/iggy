{
  config,
  lib,
  dream2nix,
  ...
}: {
  imports = [
    dream2nix.modules.dream2nix.rust-cargo-lock
    dream2nix.modules.dream2nix.rust-crane
  ];

  deps = {nixpkgs, ...}: {
    inherit
      (nixpkgs)
      stdenv
      pkg-config
      openssl
      libiconv
      darwin
      ;
  };
  env = {
    OPENSSL_NO_VENDOR = 1;
    PKG_CONFIG_PATH = "${config.deps.openssl.dev}/lib/pkgconfig";
  };
  mkDerivation = {
    src = ./.;

    nativeBuildInputs = [
      config.deps.pkg-config
    ];
    buildInputs =
      [
        config.deps.openssl
      ]
      ++ lib.optionals (config.deps.stdenv.isDarwin) [
        config.deps.libiconv
        config.deps.darwin.apple_sdk.frameworks.Security
        config.deps.darwin.apple_sdk.frameworks.SystemConfiguration
      ];
  };

  rust-crane = {
    depsDrv = {
      env = {
        OPENSSL_NO_VENDOR = 1;
        PKG_CONFIG_PATH = "${config.deps.openssl.dev}/lib/pkgconfig";
      };
      mkDerivation = {
        nativeBuildInputs = [
          config.deps.pkg-config
        ];
        buildInputs =
          [
            config.deps.openssl
          ]
          ++ lib.optionals (config.deps.stdenv.isDarwin) [
            config.deps.libiconv
            config.deps.darwin.apple_sdk.frameworks.Security
            config.deps.darwin.apple_sdk.frameworks.SystemConfiguration
          ];
      };
    };
  };

  name = "server";
  version = "0.2.24";
}
