use chrono::Utc;
use std::env;

fn main() {
    println!("cargo:rustc-env=BUILD_TIME={}", Utc::now().to_rfc2822());

    if env::var("RUSTUP_TOOLCHAIN")
        .unwrap_or_default()
        .contains("nightly")
    {
        println!("cargo:warning=Detected nightly toolchain, enabling experimental features");
        println!("cargo:rustc-cfg=nightly");
        println!("cargo:rustc-cfg=bench");
    }
}
