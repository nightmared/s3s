{ sources ? import ./sources.nix, rustTarget, rustVersion ? "stable" }:
let
  pkgs = import sources.nixpkgs { overlays = [ (import sources.nixpkgs-mozilla) ]; };
in 
pkgs.rustChannelOfTargets rustVersion null [ rustTarget ]
