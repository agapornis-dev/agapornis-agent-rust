use super::*;
use bollard::container::LogOutput;

#[derive(Default)]
pub(super) struct ConsoleOutputDecoder {
    stdout: LineBuffer,
    stderr: LineBuffer,
    console: LineBuffer,
}

impl ConsoleOutputDecoder {
    pub(super) fn push(&mut self, output: LogOutput, maximum: usize) -> Vec<String> {
        match output {
            LogOutput::StdOut { message } => self.stdout.push(&message, maximum),
            LogOutput::StdErr { message } => self.stderr.push(&message, maximum),
            LogOutput::Console { message } => self.console.push(&message, maximum),
            LogOutput::StdIn { .. } => Vec::new(),
        }
    }

    pub(super) fn finish(self) -> Vec<String> {
        let mut lines = self.stdout.finish();
        lines.extend(self.stderr.finish());
        lines.extend(self.console.finish());
        lines
    }
}

#[derive(Default)]
pub(super) struct LineBuffer {
    bytes: Vec<u8>,
    truncated: bool,
    swallow_line_feed: bool,
}

impl LineBuffer {
    pub(super) fn push(&mut self, chunk: &[u8], maximum: usize) -> Vec<String> {
        let mut lines = Vec::new();
        for byte in chunk {
            if self.swallow_line_feed {
                self.swallow_line_feed = false;
                if *byte == b'\n' {
                    continue;
                }
            }
            match *byte {
                b'\r' => {
                    lines.push(self.take_line());
                    self.swallow_line_feed = true;
                }
                b'\n' => lines.push(self.take_line()),
                byte if self.bytes.len() < maximum => self.bytes.push(byte),
                _ => self.truncated = true,
            }
        }
        lines
    }

    fn finish(mut self) -> Vec<String> {
        if self.bytes.is_empty() && !self.truncated {
            Vec::new()
        } else {
            vec![self.take_line()]
        }
    }

    fn take_line(&mut self) -> String {
        let mut line = String::from_utf8_lossy(&self.bytes).into_owned();
        self.bytes.clear();
        if std::mem::take(&mut self.truncated) {
            line.push_str(TRUNCATION_SUFFIX);
        }
        line
    }
}
