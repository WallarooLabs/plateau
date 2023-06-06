use std::env;

fn main() {
    if env::var("RUSTUP_TOOLCHAIN")
        .unwrap_or_default()
        .contains("nightly")
    {
        println!("cargo:warning=Detected nightly toolchain, enabling experimental features");
        println!("cargo:rustc-cfg=nightly");
        println!("cargo:rustc-cfg=bench");
    }
}
