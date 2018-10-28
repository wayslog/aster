debug:
	cargo build
	RUST_LOG=libass=debug RUST_BACKTRACE=full ./target/debug/asswecan

release:
	cargo build --release
	RUST_LOG=libass=info RUST_BACKTRACE=full ./target/release/asswecan

clean:
	cargo clean
