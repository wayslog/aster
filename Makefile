debug:
	cargo build
	RUST_LOG=libaster=debug RUST_BACKTRACE=full ./target/debug/aster default.toml

release:
	cargo build --release
	RUST_LOG=libaster=info RUST_BACKTRACE=full ./target/release/aster default.toml

clean:
	cargo clean
