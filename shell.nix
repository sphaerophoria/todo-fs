with import <nixpkgs> {};

mkShell {
	nativeBuildInputs = [ fuse clang-tools rustup rust-analyzer rustPlatform.bindgenHook sqlite python3];
}
