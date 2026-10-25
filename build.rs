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
    // A new commit moves HEAD or the branch it points to; the index changes
    // with `git add` and with the dirty state of tracked files.
    for f in [".git/HEAD", ".git/index", ".git/refs/heads"] {
        println!("cargo:rerun-if-changed={f}");
    }
}
