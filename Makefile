debug:
	cargo build --all
	RUST_LOG=libaster=debug RUST_BACKTRACE=full ./target/debug/aster-proxy default.toml

release:
	cargo build --all --release
	RUST_LOG=libaster=info RUST_BACKTRACE=full ./target/release/aster-proxy default.toml

clean:
	cargo clean

metrics:
	cargo build --manifest-path ./libaster/Cargo.toml --features metrics