//! Embeds the build version, `<package version>-g<commit>[-dirty]`, as
//! `BTRS3FS_VERSION` (SPEC.md §2); without git, the package version alone.

use std::process::Command;

fn main() {
    let pkg = env!("CARGO_PKG_VERSION");
    let commit = Command::new("git")
        .args([
            "describe",
            "--always",
            "--dirty",
            "--abbrev=7",
            "--exclude=*",
        ])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let version = match commit {
        Some(c) => format!("{pkg}-g{c}"),
        None => pkg.to_string(),
    };
    println!("cargo:rustc-env=BTRS3FS_VERSION={version}");
    // A new commit moves HEAD or the branch it points to, `git add` changes
    // the index. Editing a tracked file touches neither, so the sources the
    // binary is built from are watched too: an edit makes it `-dirty`.
    for f in [
        ".git/HEAD",
        ".git/index",
        ".git/refs/heads",
        "src",
        "build.rs",
        "Cargo.toml",
        "Cargo.lock",
    ] {
        println!("cargo:rerun-if-changed={f}");
    }
}
