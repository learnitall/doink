{
  description = "Distributed Objects In Kafka";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.devshell.url = "github:numtide/devshell";

  outputs = { nixpkgs, flake-utils, devshell, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
	  inherit system;
	  overlays = [
	    devshell.overlays.default
	  ];
	};
	packages = with pkgs; [
	  poetry
	  python312
	];
      in
      {
        devShells.default = pkgs.devshell.mkShell rec {
	  name = "doink";
	  inherit packages;
	  devshell.startup."printpackages" = pkgs.lib.noDepEntry ''
	    echo "[[ Packages ]]"
	    echo "${builtins.concatStringsSep "\n" (builtins.map (p: p.name) packages)}"
	    echo ""
	  '';
	};
      }
    );
}
