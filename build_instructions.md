The build process:

1. `cargo build` — compiles Rust source into a native binary (target/debug/rustream)
2. `maturin build` — runs cargo build then wraps the binary into a Python `.whl` file (a zip with metadata that pip understands)
3. `pip install ...whl`  — extracts the binary and puts it on your PATH so rustream works as a command
   (e.g. `pip install target/wheels/rustream_stream-0.1.0-py3-none-macosx_11_0_arm64.whl`)
4. `maturin develop` — shortcut that does steps 2+3 in one go (`build wheel` + install it)
5. `maturin develop --release` — optimized build, slower compile, faster binary

Every time Rust code change, need to run `maturin develop` --release to update the installed rustream command. Just `cargo build` only updates            
`./target/debug/rustream`.
