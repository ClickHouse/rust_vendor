{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  packages = [
    pkgs.rustc
    pkgs.cargo
    pkgs.z3
  ];
}
