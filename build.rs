extern crate bindgen;

use std::env::{self, var};
use std::path::{Path, PathBuf};

fn main() {
    // Tell cargo to look for shared libraries in the specified directory
    let dir = var("CARGO_MANIFEST_DIR").unwrap();
    println!(
        "cargo:rustc-link-search={}",
        Path::new(&dir).join("lib").display()
    );
    println!("cargo:rustc-link-lib=dylib=cstore_fdw");
    println!("cargo:rustc-link-lib=dylib=postgres");

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=includes.h");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        .allowlist_function("CStore.*")
        .header("includes.h")
        .clang_arg(&"-I./include".to_string())
        .rustfmt_bindings(true)
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
