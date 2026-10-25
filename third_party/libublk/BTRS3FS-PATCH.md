# libublk 0.4.8 with a local patch

Copy of `libublk` 0.4.8 from crates.io, used through `[patch.crates-io]` in the
top-level `Cargo.toml`. The only change: `UBLK_F_USER_RECOVERY_FAIL_IO` is added to
`UBLK_DRV_F_ALL` (so `validate_new_params()` accepts it) and the `FAIL_IO` state is
named in `dev_state_desc()`. Why: `SPEC.md` §9.4.

Upstream: ublk-org/libublk-rs, branch `accept-recovery-fail-io`. Remove this
directory and the `[patch]` section once a release contains the change.
