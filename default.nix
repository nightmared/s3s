{ sources ? import ./nix/sources.nix}:

let
  rustTarget = "x86_64-unknown-linux-musl";
  rust = import ./nix/rust.nix { inherit sources rustTarget; };
  pkgs = import sources.nixpkgs {
    overlays = [ (import "${sources.nixpkgs-mozilla}/rust-overlay.nix") (import "${sources.nixpkgs-mozilla}/rust-src-overlay.nix") (import "${sources.cargo2nix}/overlay") ];
    crossSystem = {
      config = rustTarget;
    };
  };

  rustPkgs = pkgs.rustBuilder.makePackageSet' {
    rustChannel = "1.49.0";
    packageFun = import ./Cargo.nix;
  };
in
rustPkgs.workspace.s3s {}
