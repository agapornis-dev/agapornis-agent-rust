//! Runtime rate limits and unsafe-console-output filtering.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

pub struct ProtectionState {
    commands: Mutex<HashMap<String, Rate>>,
    console: Mutex<HashMap<String, ConsoleRate>>,
    statuses: Mutex<HashMap<String, String>>,
    recovery: Mutex<HashMap<String, Instant>>,
    console_lines_per_second: usize,
    console_strike_limit: usize,
    console_max_line_length: usize,
    console_commands_per_5_seconds: usize,
}

impl Default for ProtectionState {
    fn default() -> Self {
        Self {
            commands: Mutex::new(HashMap::new()),
            console: Mutex::new(HashMap::new()),
            statuses: Mutex::new(HashMap::new()),
            recovery: Mutex::new(HashMap::new()),
            // Configuration is process-scoped. Read it once at daemon startup
            // instead of consulting the environment for every console line.
            console_lines_per_second: env_usize("AGAPORNIS_CONSOLE_LINES_PER_SECOND", 2000),
            console_strike_limit: env_usize("AGAPORNIS_CONSOLE_STRIKE_LIMIT", 3),
            console_max_line_length: env_usize("AGAPORNIS_CONSOLE_MAX_LINE_LENGTH", 16384),
            console_commands_per_5_seconds: env_usize(
                "AGAPORNIS_CONSOLE_COMMANDS_PER_5_SECONDS",
                20,
            ),
        }
    }
}

#[derive(Clone)]
struct Rate {
    started: Instant,
    count: usize,
}

struct ConsoleRate {
    started: Instant,
    count: usize,
    strikes: usize,
    last_strike: Option<Instant>, // Added to track the 10-second cooldown accurately
    exceeded: bool,
}

pub enum LineDecision {
    Allow,
    Suppress,
    Notify,
    Trip,
}

impl ProtectionState {
    pub fn observe_line(&self, id: &str) -> LineDecision {
        let mut rates = self.console.lock().unwrap();

        let rate = rates.entry(id.into()).or_insert_with(|| ConsoleRate {
            started: Instant::now(),
            count: 0,
            strikes: 0,
            last_strike: None,
            exceeded: false,
        });

        if let Some(ls) = rate.last_strike
            && ls.elapsed() >= Duration::from_secs(10)
        {
            rate.strikes = 0;
            rate.last_strike = None;
        }

        if rate.started.elapsed() >= Duration::from_secs(1) {
            rate.started = Instant::now();
            rate.count = 0;
            rate.exceeded = false;
        }

        rate.count += 1;

        if rate.count <= self.console_lines_per_second {
            return LineDecision::Allow;
        }

        if rate.exceeded {
            return LineDecision::Suppress;
        }

        rate.exceeded = true;
        rate.strikes += 1;
        rate.last_strike = Some(Instant::now());

        if rate.strikes >= self.console_strike_limit {
            LineDecision::Trip
        } else if rate.strikes == 1 {
            // Only send the Notify warning on the FIRST strike to prevent duplicated messages
            LineDecision::Notify
        } else {
            // Suppress on intermediate strikes (e.g., strike 2) so they don't get spammed
            LineDecision::Suppress
        }
    }

    pub fn max_line_length(&self) -> usize {
        self.console_max_line_length
    }

    pub fn console_lines_per_second(&self) -> usize {
        self.console_lines_per_second
    }

    pub fn accept_command(&self, id: &str) -> bool {
        let mut rates = self.commands.lock().unwrap();
        let rate = rates.entry(id.to_owned()).or_insert(Rate {
            started: Instant::now(),
            count: 0,
        });
        if rate.started.elapsed() >= Duration::from_secs(5) {
            rate.started = Instant::now();
            rate.count = 0;
        }
        rate.count += 1;
        rate.count <= self.console_commands_per_5_seconds
    }

    pub fn status(&self, id: &str) -> Option<String> {
        self.statuses.lock().unwrap().get(id).cloned()
    }

    pub fn mark(&self, id: &str, status: &str) {
        self.statuses
            .lock()
            .unwrap()
            .insert(id.into(), status.into());
    }

    pub fn clear_disk(&self, id: &str) {
        let mut s = self.statuses.lock().unwrap();
        if s.get(id).is_some_and(|v| v == "disk-limit-exceeded") {
            s.remove(id);
        }
    }

    pub fn manual_recovery(&self, id: &str) {
        self.statuses.lock().unwrap().remove(id);
        self.recovery
            .lock()
            .unwrap()
            .insert(id.into(), Instant::now() + Duration::from_secs(120));
    }

    pub fn in_manual_recovery(&self, id: &str) -> bool {
        let mut recovery = self.recovery.lock().unwrap();
        match recovery.get(id).copied() {
            Some(until) if until > Instant::now() => true,
            Some(_) => {
                recovery.remove(id);
                false
            }
            None => false,
        }
    }

    pub fn remove(&self, id: &str) {
        self.commands.lock().unwrap().remove(id);
        self.console.lock().unwrap().remove(id);
        self.statuses.lock().unwrap().remove(id);
        self.recovery.lock().unwrap().remove(id);
    }
}

pub fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(fallback)
}
