debug:
	cargo build
	RUST_LOG=libaster=debug RUST_BACKTRACE=full ./target/debug/aster configs/default.toml.example

release:
	cargo build --release
	RUST_LOG=libaster=info RUST_BACKTRACE=full ./target/release/aster configs/default.toml.example

clean:
	cargo clean
