//! The connection regulator (SPEC.md §6.1): finds how many parallel uploads
//! (or GETs) the link takes by watching the throughput while all slots are
//! busy. Slow start doubles from 1; a doubling without gain goes back to the
//! best; then a probe +1 every 30 s under load; a drop or retries halve.

use crate::tuning::CONNECTIONS_MAX;

/// Growth a doubling must bring in slow start.
const DOUBLING_GAIN: f64 = 1.10;
/// Growth a probe +1 must bring to be kept.
const PROBE_GAIN: f64 = 1.05;
/// Seconds between probes under load.
const PROBE_EVERY: f64 = 30.0;
/// Throughput below this share of the best at the same load: the link got worse.
const DROP: f64 = 2.0 / 3.0;
/// Weight of a new steady window in the best throughput (smoothing).
const STEADY_WEIGHT: f64 = 0.2;
/// A decision needs a window of at least this many saturated seconds with at
/// least `WINDOW_COMPLETIONS` requests finished in it: bytes count when a
/// request completes (an upload of a whole zone may take seconds), so one
/// second alone is noise.
const WINDOW_SECS: f64 = 3.0;
const WINDOW_COMPLETIONS: u64 = 3;

/// One step's view of the link in one direction.
#[derive(Clone, Copy, Debug)]
pub struct Sample {
    pub bytes_per_sec: f64,
    /// All slots were busy most of the step: the limit, not the demand, set the rate.
    pub saturated: bool,
    /// Requests that finished in the step.
    pub completions: u64,
    pub retries: u64,
    pub now_secs: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Phase {
    /// Doubling; the rate at the limit before the last doubling.
    SlowStart {
        before: Option<f64>,
    },
    Steady,
    /// One more slot on trial; the limit and rate before it.
    Probing {
        from: usize,
        base: f64,
    },
}

pub struct Regulator {
    limit: usize,
    phase: Phase,
    best: Option<(usize, f64)>,
    next_probe: f64,
    last_change: Option<(f64, String)>,
    /// The window being gathered at the current limit: (seconds, bytes, completions).
    window: (f64, f64, u64),
}

impl Default for Regulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Regulator {
    pub fn new() -> Self {
        Self {
            limit: 1,
            phase: Phase::SlowStart { before: None },
            best: None,
            next_probe: 0.0,
            last_change: None,
            window: (0.0, 0.0, 0),
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Throughput at the best limit found: the estimate of the whole link.
    pub fn best_rate(&self) -> Option<f64> {
        self.best.map(|(_, r)| r)
    }

    pub fn phase(&self) -> &'static str {
        match self.phase {
            Phase::SlowStart { .. } => "slow start",
            Phase::Steady => "steady",
            Phase::Probing { .. } => "probing",
        }
    }

    /// When the limit last changed and why.
    pub fn last_change(&self) -> Option<&(f64, String)> {
        self.last_change.as_ref()
    }

    /// Starts over from `n` (after a measurement).
    pub fn restart_from(&mut self, n: usize) {
        self.window = (0.0, 0.0, 0);
        self.limit = n.clamp(1, CONNECTIONS_MAX);
        self.phase = Phase::SlowStart { before: None };
        self.best = None;
    }

    fn change(&mut self, now: f64, to: usize, why: String) {
        let to = to.clamp(1, CONNECTIONS_MAX);
        // A new limit, a new window: the old one says nothing about it.
        self.window = (0.0, 0.0, 0);
        if to != self.limit {
            self.last_change = Some((now, format!("{} → {to}: {why}", self.limit)));
            self.limit = to;
        }
    }

    fn settle(&mut self, now: f64) {
        self.phase = Phase::Steady;
        self.next_probe = now + PROBE_EVERY;
    }

    /// Halves the limit and starts over: the link got worse.
    fn back_off(&mut self, now: f64, why: String) {
        self.change(now, self.limit / 2, why);
        self.phase = Phase::SlowStart { before: None };
        self.best = None;
    }

    /// Feeds one step; returns the limit to apply.
    pub fn step(&mut self, s: Sample) -> usize {
        let now = s.now_secs;
        if s.retries > 0 {
            self.back_off(now, format!("{} retries", s.retries));
            return self.limit;
        }
        if !s.saturated {
            // Demand, not the limit, set this rate: start the window over.
            self.window = (0.0, 0.0, 0);
            return self.limit;
        }
        let w = &mut self.window;
        w.0 += 1.0;
        w.1 += s.bytes_per_sec;
        w.2 += s.completions;
        if w.0 < WINDOW_SECS || w.2 < WINDOW_COMPLETIONS {
            return self.limit;
        }
        let rate = w.1 / w.0;
        self.window = (0.0, 0.0, 0);
        match self.phase {
            Phase::SlowStart { before } => {
                if before.is_none_or(|b| rate >= b * DOUBLING_GAIN) {
                    self.best = Some((self.limit, rate));
                    if self.limit >= CONNECTIONS_MAX {
                        self.settle(now);
                    } else {
                        self.phase = Phase::SlowStart { before: Some(rate) };
                        self.change(now, self.limit * 2, "slow start".into());
                    }
                } else {
                    let back = self.best.map_or(1, |(n, _)| n);
                    self.change(now, back, "doubling gave no gain".into());
                    self.settle(now);
                }
            }
            Phase::Steady => {
                let best = self.best.map_or(rate, |(_, r)| r);
                if rate < best * DROP {
                    self.back_off(
                        now,
                        format!(
                            "throughput fell to {:.0}% of {:.1} MiB/s",
                            rate / best * 100.0,
                            best / (1 << 20) as f64
                        ),
                    );
                    return self.limit;
                }
                let smoothed = best + STEADY_WEIGHT * (rate - best);
                self.best = Some((self.limit, smoothed));
                if now >= self.next_probe && self.limit < CONNECTIONS_MAX {
                    self.phase = Phase::Probing {
                        from: self.limit,
                        base: smoothed,
                    };
                    self.change(now, self.limit + 1, "probe".into());
                }
            }
            Phase::Probing { from, base } => {
                if rate >= base * PROBE_GAIN {
                    self.best = Some((self.limit, rate));
                    self.last_change = Some((now, format!("{from} → {}: probe kept", self.limit)));
                } else {
                    self.change(now, from, "probe gave no gain".into());
                }
                self.settle(now);
            }
        }
        self.limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: f64 = (1 << 20) as f64;

    /// A link of `total` bytes/s where one connection moves `stream`.
    struct Link {
        stream: f64,
        total: f64,
    }

    impl Link {
        fn rate(&self, n: usize) -> f64 {
            (n as f64 * self.stream).min(self.total)
        }
    }

    /// Steps `r` once per simulated second for `secs` seconds, saturated.
    fn run(r: &mut Regulator, link: &Link, t: &mut f64, secs: usize) {
        for _ in 0..secs {
            *t += 1.0;
            let rate = link.rate(r.limit());
            r.step(Sample {
                bytes_per_sec: rate,
                saturated: true,
                completions: 10,
                retries: 0,
                now_secs: *t,
            });
        }
    }

    #[test]
    fn grows_to_saturation() {
        let mut r = Regulator::new();
        assert_eq!(r.limit(), 1);
        let link = Link {
            stream: 3.0 * MIB,
            total: 24.0 * MIB,
        };
        let mut t = 0.0;
        run(&mut r, &link, &mut t, 30);
        assert!((8..=16).contains(&r.limit()), "{}", r.limit());
        let best = r.best_rate().unwrap();
        assert!((best - 24.0 * MIB).abs() < 2.4 * MIB, "{best}");
    }

    #[test]
    fn doubling_without_gain_reverts() {
        let mut r = Regulator::new();
        let link = Link {
            stream: 12.0 * MIB,
            total: 24.0 * MIB,
        };
        let mut t = 0.0;
        run(&mut r, &link, &mut t, 30);
        assert_eq!(r.limit(), 2);
    }

    #[test]
    fn probe_every_30s() {
        let mut r = Regulator::new();
        let link = Link {
            stream: 12.0 * MIB,
            total: 24.0 * MIB,
        };
        let mut t = 0.0;
        run(&mut r, &link, &mut t, 30);
        let mut changes = Vec::new();
        let mut last = r.limit();
        for _ in 0..120 {
            run(&mut r, &link, &mut t, 1);
            if r.limit() != last {
                changes.push(t);
                last = r.limit();
            }
        }
        // A probe up and its revert each 30 s: never two probes closer than 30 s.
        let ups: Vec<f64> = changes.iter().step_by(2).copied().collect();
        assert!(ups.len() >= 3, "{changes:?}");
        assert!(ups.windows(2).all(|w| w[1] - w[0] >= 30.0), "{changes:?}");
        assert_eq!(r.limit(), 2); // probes without gain revert
    }

    #[test]
    fn probe_with_gain_is_kept() {
        let mut r = Regulator::new();
        // Grows slowly: doubling 8 → 16 gains only 6 %, below the 10 % of
        // slow start, but each probe +1 gains more than 5 %.
        let mut t = 0.0;
        let link = Link {
            stream: 1.0 * MIB,
            total: 10.0 * MIB,
        };
        run(&mut r, &link, &mut t, 30);
        let settled = r.limit();
        assert!(settled >= 8, "{settled}");
        let link = Link {
            stream: 1.0 * MIB,
            total: 12.0 * MIB,
        };
        run(&mut r, &link, &mut t, 100);
        assert!(r.limit() > settled.min(10), "{} {settled}", r.limit());
    }

    #[test]
    fn drop_halves() {
        let mut r = Regulator::new();
        let mut t = 0.0;
        run(
            &mut r,
            &Link {
                stream: 3.0 * MIB,
                total: 24.0 * MIB,
            },
            &mut t,
            10,
        );
        let before = r.limit();
        let vpn = Link {
            stream: 3.0 * MIB,
            total: 6.0 * MIB,
        };
        run(&mut r, &vpn, &mut t, 4);
        assert!(r.limit() <= before / 2, "{} of {before}", r.limit());
        run(&mut r, &vpn, &mut t, 30);
        assert!((2..=4).contains(&r.limit()), "{}", r.limit());
        assert_eq!(r.phase(), "steady");
    }

    #[test]
    fn retry_halves() {
        let mut r = Regulator::new();
        let mut t = 0.0;
        run(
            &mut r,
            &Link {
                stream: 3.0 * MIB,
                total: 24.0 * MIB,
            },
            &mut t,
            10,
        );
        let before = r.limit();
        r.step(Sample {
            bytes_per_sec: 24.0 * MIB,
            saturated: true,
            completions: 10,
            retries: 2,
            now_secs: t + 1.0,
        });
        assert_eq!(r.limit(), (before / 2).max(1));
        assert!(r.last_change().unwrap().1.contains("retr"));
    }

    #[test]
    fn idle_freezes() {
        let mut r = Regulator::new();
        let mut t = 0.0;
        run(
            &mut r,
            &Link {
                stream: 3.0 * MIB,
                total: 24.0 * MIB,
            },
            &mut t,
            10,
        );
        let (limit, best) = (r.limit(), r.best_rate());
        for i in 0..100 {
            r.step(Sample {
                bytes_per_sec: 0.0,
                saturated: false,
                completions: 10,
                retries: 0,
                now_secs: t + i as f64,
            });
        }
        assert_eq!((r.limit(), r.best_rate()), (limit, best));
    }

    /// Uploads count when they complete: a slow link has saturated seconds
    /// with nothing finished. Those say nothing, so nothing changes.
    #[test]
    fn steps_without_completions_are_no_evidence() {
        let sample = |rate: f64, completions: u64, t: f64| Sample {
            bytes_per_sec: rate,
            saturated: true,
            completions,
            retries: 0,
            now_secs: t,
        };
        let mut r = Regulator::new();
        for i in 0..60 {
            r.step(sample(0.0, 0, i as f64));
        }
        assert_eq!(r.limit(), 1);
        assert_eq!(r.best_rate(), None);
        // Once uploads complete, slow start goes on.
        for i in 60..70 {
            r.step(sample(3.0 * MIB, 1, i as f64));
        }
        assert!(r.limit() >= 2, "{}", r.limit());
    }

    /// One completion per second is noise: the rate is judged over windows
    /// of several completions and several seconds.
    #[test]
    fn decisions_wait_for_a_window() {
        let sample = |t: f64| Sample {
            bytes_per_sec: 3.0 * MIB,
            saturated: true,
            completions: 5,
            retries: 0,
            now_secs: t,
        };
        let mut r = Regulator::new();
        r.step(sample(0.0));
        r.step(sample(1.0));
        assert_eq!(r.limit(), 1, "decided before a whole window");
        r.step(sample(2.0));
        assert_eq!(r.limit(), 2);
    }

    #[test]
    fn cap_64() {
        let mut r = Regulator::new();
        let mut t = 0.0;
        run(
            &mut r,
            &Link {
                stream: MIB,
                total: f64::INFINITY,
            },
            &mut t,
            300,
        );
        assert_eq!(r.limit(), 64);
    }

    #[test]
    fn restart_from_measurement() {
        let mut r = Regulator::new();
        r.restart_from(12);
        assert_eq!(r.limit(), 12);
        assert_eq!(r.phase(), "slow start");
    }
}
