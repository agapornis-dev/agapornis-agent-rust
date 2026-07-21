use super::*;
use std::mem::size_of;

pub(super) const CONSOLE_HISTORY_LINES: usize = 200;
pub(super) const CONSOLE_HISTORY_BYTES: usize = 64 * 1024;
const DEFAULT_TOTAL_HISTORY_BYTES: usize = 64 * 1024 * 1024;
const MAX_TOTAL_HISTORY_BYTES: usize = 1024 * 1024 * 1024;

#[derive(Default)]
pub(super) struct ConsoleHistory {
    pub(super) lines: VecDeque<ConsoleEntry>,
    pub(super) content_bytes: usize,
    charged_bytes: usize,
    next_sequence: u64,
    last_access: u64,
}

pub(super) struct HistoryCache {
    entries: HashMap<String, ConsoleHistory>,
    charged_bytes: usize,
    maximum_bytes: usize,
    clock: u64,
}

impl HistoryCache {
    pub(super) fn configured() -> Self {
        let maximum = std::env::var("AGAPORNIS_CONSOLE_HISTORY_TOTAL_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_TOTAL_HISTORY_BYTES)
            .clamp(CONSOLE_HISTORY_BYTES, MAX_TOTAL_HISTORY_BYTES);
        Self::with_maximum(maximum)
    }

    pub(super) fn with_maximum(maximum_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            charged_bytes: 0,
            maximum_bytes: maximum_bytes.max(1),
            clock: 0,
        }
    }

    pub(super) fn push(&mut self, id: &str, line: String) -> ConsoleEntry {
        self.clock = self.clock.saturating_add(1);
        let access = self.clock;
        let entry = {
            let history = self.entries.entry(id.to_owned()).or_default();
            history.last_access = access;
            history.next_sequence = history.next_sequence.saturating_add(1);
            let entry = ConsoleEntry {
                sequence: history.next_sequence,
                line: line.into(),
            };
            let charge = entry_charge(&entry);
            history.content_bytes = history.content_bytes.saturating_add(entry.line.len());
            history.charged_bytes = history.charged_bytes.saturating_add(charge);
            self.charged_bytes = self.charged_bytes.saturating_add(charge);
            history.lines.push_back(entry.clone());

            while history.lines.len() > CONSOLE_HISTORY_LINES
                || history.content_bytes > CONSOLE_HISTORY_BYTES
            {
                let Some(removed) = history.lines.pop_front() else {
                    break;
                };
                let charge = entry_charge(&removed);
                history.content_bytes = history.content_bytes.saturating_sub(removed.line.len());
                history.charged_bytes = history.charged_bytes.saturating_sub(charge);
                self.charged_bytes = self.charged_bytes.saturating_sub(charge);
            }
            entry
        };
        self.evict_to_budget(id);
        entry
    }

    pub(super) fn snapshot(&mut self, id: &str) -> (Vec<String>, u64) {
        self.clock = self.clock.saturating_add(1);
        let Some(history) = self.entries.get_mut(id) else {
            return Default::default();
        };
        history.last_access = self.clock;
        (
            history
                .lines
                .iter()
                .map(|entry| entry.line.to_string())
                .collect(),
            history
                .lines
                .back()
                .map(|entry| entry.sequence)
                .unwrap_or(0),
        )
    }

    pub(super) fn remove(&mut self, id: &str) {
        if let Some(history) = self.entries.remove(id) {
            self.charged_bytes = self.charged_bytes.saturating_sub(history.charged_bytes);
        }
    }

    fn evict_to_budget(&mut self, protected_id: &str) {
        while self.charged_bytes > self.maximum_bytes && self.entries.len() > 1 {
            let oldest = self
                .entries
                .iter()
                .filter(|(id, _)| id.as_str() != protected_id)
                .min_by_key(|(_, history)| history.last_access)
                .map(|(id, _)| id.clone());
            let Some(oldest) = oldest else {
                break;
            };
            self.remove(&oldest);
        }

        let Some(history) = self.entries.get_mut(protected_id) else {
            return;
        };
        while self.charged_bytes > self.maximum_bytes {
            let Some(removed) = history.lines.pop_front() else {
                break;
            };
            let charge = entry_charge(&removed);
            history.content_bytes = history.content_bytes.saturating_sub(removed.line.len());
            history.charged_bytes = history.charged_bytes.saturating_sub(charge);
            self.charged_bytes = self.charged_bytes.saturating_sub(charge);
        }
    }

    #[cfg(test)]
    pub(super) fn contains_key(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    pub(super) fn get(&self, id: &str) -> Option<&ConsoleHistory> {
        self.entries.get(id)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(super) fn charged_bytes(&self) -> usize {
        self.charged_bytes
    }
}

fn entry_charge(entry: &ConsoleEntry) -> usize {
    size_of::<ConsoleEntry>().saturating_add(entry.line.len())
}
