//! Logging: journald, or syslog through /dev/log when journald is absent;
//! optionally mirrored to stderr (colored when stderr is a terminal).
//! Never to files (SPEC.md §9.3).

use log::{Log, Metadata, Record};
use std::path::Path;

/// Log target of the per-request trace (`--log-level info,requests=trace`).
/// Its records go to stderr only: thousands of lines per second would hit the
/// journald rate limit and drown everything else.
pub const REQUESTS_TARGET: &str = "requests";

/// SYSLOG_IDENTIFIER of our journal records: `journalctl -t btrs3fs`.
const IDENTIFIER: &str = "btrs3fs";
/// Native journald protocol socket (systemd's fixed path); if absent, there is
/// no journald and syslog through /dev/log is used.
const JOURNAL_SOCKET: &str = "/run/systemd/journal/socket";

struct Tee {
    /// Decides which records pass (`--log-level`); never writes itself.
    filter: env_logger::Logger,
    system: Option<Box<dyn Log>>,
    stderr: Option<env_logger::Logger>,
}

impl Log for Tee {
    fn enabled(&self, metadata: &Metadata) -> bool {
        self.filter.enabled(metadata)
    }

    fn log(&self, record: &Record) {
        if !self.filter.matches(record) {
            return;
        }
        if let Some(l) = self
            .system
            .as_ref()
            .filter(|_| record.target() != REQUESTS_TARGET)
        {
            l.log(record);
        }
        if let Some(l) = &self.stderr {
            l.log(record);
        }
    }

    fn flush(&self) {
        if let Some(l) = &self.system {
            l.flush();
        }
        if let Some(l) = &self.stderr {
            l.flush();
        }
    }
}

fn system_logger() -> Option<Box<dyn Log>> {
    if Path::new(JOURNAL_SOCKET).exists() {
        match systemd_journal_logger::JournalLog::new() {
            Ok(j) => return Some(Box::new(j.with_syslog_identifier(IDENTIFIER.into()))),
            Err(e) => eprintln!("{IDENTIFIER}: cannot log to journald: {e}"),
        }
    }
    let formatter = syslog::Formatter3164 {
        facility: syslog::Facility::LOG_DAEMON,
        hostname: None,
        process: IDENTIFIER.into(),
        pid: std::process::id(),
    };
    match syslog::unix(formatter) {
        Ok(l) => Some(Box::new(syslog::BasicLogger::new(l))),
        Err(e) => {
            eprintln!("{IDENTIFIER}: cannot log to journald or /dev/log: {e}");
            None
        }
    }
}

/// Installs the global logger. `level` uses env_logger filter syntax.
pub fn init(level: &str, to_stderr: bool) {
    let filter = env_logger::Builder::new().parse_filters(level).build();
    let stderr = to_stderr.then(|| {
        env_logger::Builder::new()
            .parse_filters(level)
            .format_target(false)
            .build()
    });
    log::set_max_level(filter.filter());
    let tee = Tee {
        filter,
        system: system_logger(),
        stderr,
    };
    log::set_boxed_logger(Box::new(tee)).expect("logger is installed once");
}
