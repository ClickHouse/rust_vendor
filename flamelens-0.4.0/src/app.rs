use crate::flame::{FlameGraph, SearchPattern};
#[cfg(feature = "python")]
use crate::py_spy::{record_samples, ProfilerOutput, SamplerState, SamplerStatus};
use crate::state::FlameGraphState;
use crate::view::FlameGraphView;
#[cfg(feature = "python")]
use remoteprocess;
use std::collections::HashMap;
use std::error;
use std::sync::{Arc, Mutex};
#[cfg(feature = "python")]
use std::thread;
use std::time::Duration;

/// Application result type.
pub type AppResult<T> = std::result::Result<T, Box<dyn error::Error>>;

#[derive(Debug)]
pub enum FlameGraphInput {
    File(String),
    Pid(u64, Option<String>),
}

#[derive(Debug)]
pub struct ParsedFlameGraph {
    pub flamegraph: FlameGraph,
    pub elapsed: Duration,
}

#[derive(Debug)]
pub struct InputBuffer {
    pub buffer: tui_input::Input,
    pub cursor: Option<(u16, u16)>,
}

/// Application.
#[derive(Debug)]
pub struct App {
    /// Is the application running?
    pub running: bool,
    /// Flamegraph view
    pub flamegraph_view: FlameGraphView,
    /// Flamegraph input information
    pub flamegraph_input: FlameGraphInput,
    /// User input buffer
    pub input_buffer: Option<InputBuffer>,
    /// Timing information for debugging
    pub elapsed: HashMap<String, Duration>,
    /// Transient message
    pub transient_message: Option<String>,
    /// Debug mode
    pub debug: bool,
    /// Baseline ("before") flamegraph of diff mode, kept to recompute the diff
    /// when the stack order is toggled
    diff_baseline: Option<FlameGraph>,
    /// Next flamegraph to swap in
    next_flamegraph: Arc<Mutex<Option<ParsedFlameGraph>>>,
    #[cfg(feature = "python")]
    sampler_state: Option<Arc<Mutex<SamplerState>>>,
}

impl App {
    /// Constructs a new instance of [`App`].
    pub fn with_flamegraph(filename: &str, flamegraph: FlameGraph) -> Self {
        Self {
            running: true,
            flamegraph_view: FlameGraphView::new(flamegraph),
            flamegraph_input: FlameGraphInput::File(filename.to_string()),
            input_buffer: None,
            elapsed: HashMap::new(),
            transient_message: None,
            debug: false,
            diff_baseline: None,
            next_flamegraph: Arc::new(Mutex::new(None)),
            #[cfg(feature = "python")]
            sampler_state: None,
        }
    }

    #[cfg(feature = "python")]
    pub fn with_pid(pid: u64, py_spy_args: Option<String>) -> Self {
        let next_flamegraph: Arc<Mutex<Option<ParsedFlameGraph>>> = Arc::new(Mutex::new(None));
        let pyspy_data: Arc<Mutex<Option<ProfilerOutput>>> = Arc::new(Mutex::new(None));
        let sampler_state = Arc::new(Mutex::new(SamplerState::default()));

        // Thread to poll data from pyspy and construct the next flamegraph
        {
            let next_flamegraph = next_flamegraph.clone();
            let pyspy_data = pyspy_data.clone();
            let _handle = thread::spawn(move || loop {
                if let Some(output) = pyspy_data.lock().unwrap().take() {
                    let tic = std::time::Instant::now();
                    let flamegraph = FlameGraph::from_string(output.data, true);
                    let parsed = ParsedFlameGraph {
                        flamegraph,
                        elapsed: tic.elapsed(),
                    };
                    *next_flamegraph.lock().unwrap() = Some(parsed);
                }
                thread::sleep(std::time::Duration::from_millis(250));
            });
        }

        // pyspy live sampler thread
        {
            let pyspy_data = pyspy_data.clone();
            let sampler_state = sampler_state.clone();
            let _handle = thread::spawn(move || {
                // Note: mimic a record command's invocation vs simply getting default Config as
                // from_args does a lot of heavy lifting
                let mut args = [
                    "py-spy",
                    "record",
                    "--pid",
                    pid.to_string().as_str(),
                    "--format",
                    "raw",
                ]
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>();
                if let Some(py_spy_args) = py_spy_args {
                    args.extend(py_spy_args.split_whitespace().map(|s| s.to_string()));
                }
                let config = py_spy::Config::from_args(&args).unwrap();
                let pid = pid as remoteprocess::Pid;
                record_samples(pid, &config, pyspy_data, sampler_state);
            });
        }

        let flamegraph = FlameGraph::from_string("".to_string(), true);
        let process_info = remoteprocess::Process::new(pid as remoteprocess::Pid)
            .and_then(|p| p.cmdline())
            .ok()
            .map(|c| c.join(" "));
        Self {
            running: true,
            flamegraph_view: FlameGraphView::new(flamegraph),
            flamegraph_input: FlameGraphInput::Pid(pid, process_info),
            next_flamegraph: next_flamegraph.clone(),
            input_buffer: None,
            elapsed: HashMap::new(),
            transient_message: None,
            debug: false,
            diff_baseline: None,
            sampler_state: Some(sampler_state),
        }
    }

    pub fn set_diff_baseline(&mut self, before: FlameGraph) {
        self.flamegraph_view.flamegraph.set_diff_against(&before);
        self.diff_baseline = Some(before);
    }

    pub fn toggle_reversed(&mut self) {
        self.flamegraph_view.toggle_reversed();
        // Recompute the diff in the new orientation: full names changed, so the
        // baseline must be reversed the same way for them to match up.
        if let Some(baseline) = &self.diff_baseline {
            let flamegraph = &mut self.flamegraph_view.flamegraph;
            if flamegraph.reversed {
                flamegraph.set_diff_against(&baseline.to_reversed());
            } else {
                flamegraph.set_diff_against(baseline);
            }
        }
    }

    /// Handles the tick event of the terminal.
    pub fn tick(&mut self) {
        // Replace flamegraph
        if !self.flamegraph_view.state.freeze {
            if let Some(parsed) = self.next_flamegraph.lock().unwrap().take() {
                self.elapsed
                    .insert("flamegraph".to_string(), parsed.elapsed);
                let tic = std::time::Instant::now();
                self.flamegraph_view.replace_flamegraph(parsed.flamegraph);
                self.elapsed
                    .insert("replacement".to_string(), tic.elapsed());
            }
        }

        // Exit if fatal error in sampler
        #[cfg(feature = "python")]
        if let Some(SamplerStatus::Error(s)) = self
            .sampler_state
            .as_ref()
            .map(|s| s.lock().unwrap().status.clone())
        {
            panic!("py-spy sampler exited with error: {}\n\nYou likely need to rerun this program with sudo.", s);
        }
    }

    /// Set running to false to quit the application.
    pub fn quit(&mut self) {
        self.running = false;
    }

    pub fn flamegraph(&self) -> &FlameGraph {
        &self.flamegraph_view.flamegraph
    }

    pub fn flamegraph_state(&self) -> &FlameGraphState {
        &self.flamegraph_view.state
    }

    #[cfg(feature = "python")]
    pub fn sampler_state(&self) -> Option<SamplerState> {
        self.sampler_state
            .as_ref()
            .map(|s| s.lock().unwrap().clone())
    }

    pub fn add_elapsed(&mut self, name: &str, elapsed: Duration) {
        self.elapsed.insert(name.to_string(), elapsed);
    }

    pub fn search_selected(&mut self) {
        if self.flamegraph_view.is_root_selected() {
            return;
        }
        let short_name = self.flamegraph_view.get_selected_stack().map(|s| {
            self.flamegraph()
                .get_stack_short_name_from_info(s)
                .to_string()
        });
        if let Some(short_name) = short_name {
            self.set_manual_search_pattern(short_name.as_str(), false);
        }
    }

    pub fn search_selected_row(&mut self) {
        let short_name = self
            .flamegraph_view
            .get_selected_row_name()
            .map(|s| s.to_string());
        if let Some(short_name) = short_name {
            self.set_manual_search_pattern(short_name.as_str(), false);
        }
        self.flamegraph_view.state.toggle_view_kind();
    }

    pub fn set_manual_search_pattern(&mut self, pattern: &str, is_regex: bool) {
        match SearchPattern::new(pattern, is_regex, true) {
            Ok(p) => self.flamegraph_view.set_search_pattern(p),
            Err(_) => {
                self.set_transient_message(&format!("Invalid regex: {}", pattern));
            }
        }
    }

    pub fn set_transient_message(&mut self, message: &str) {
        self.transient_message = Some(message.to_string());
    }

    pub fn clear_transient_message(&mut self) {
        self.transient_message = None;
    }

    pub fn toggle_debug(&mut self) {
        self.debug = !self.debug;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_toggle_reversed_recomputes_diff() {
        let before = "a;x 10\nb;x 20\n";
        let after = "a;x 10\nb;x 50\n";
        let fg = FlameGraph::from_string(after.to_string(), false);
        let mut app = App::with_flamegraph("test", fg);
        app.set_diff_baseline(FlameGraph::from_string(before.to_string(), false));

        let diff_of = |app: &App, full: &str| {
            app.flamegraph()
                .get_stack_by_full_name(full)
                .unwrap()
                .diff
                .unwrap()
        };
        assert!(app.flamegraph().diff_mode);
        assert_eq!(diff_of(&app, "b;x"), 30);

        app.toggle_reversed();
        assert!(app.flamegraph().reversed);
        assert!(app.flamegraph().diff_mode);
        assert_eq!(diff_of(&app, "x;b"), 30);
        assert_eq!(diff_of(&app, "x;a"), 0);
        assert_eq!(app.flamegraph().max_abs_diff, 30);

        app.toggle_reversed();
        assert!(!app.flamegraph().reversed);
        assert_eq!(diff_of(&app, "b;x"), 30);
    }
}
