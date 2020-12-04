#[path = "./paths.rs"]
mod paths;
use paths::{InputPath, OutputPath};

#[allow(dead_code)]
pub struct Common {
    /// State name
    pub name:        String,
    /// All states MUST have a "Type" field. This document refers to the values
    /// of this field as a state’s type, and to a state such as the one in the
    /// example above as a Task State.
    /// FIXME: we use `family` to replace `type` keyword reserved by rust.
    pub family:      String,
    /// Any state MAY have a "Comment" field, to hold a human-readable comment
    /// or description.
    pub comment:     Option<String>,
    /// Transitions link states together, defining the control flow for the
    /// state machine. After executing a non-terminal state, the interpreter
    /// follows a transition to the next state. For most state types,
    /// transitions are unconditional and specified through the state's "Next"
    /// field.
    pub next:        String,
    /// Designates this state as a terminal state (ends the execution) if set to
    /// true. There can be any number of terminal states per state machine. Only
    /// one of Next or End can be used in a state. Some state types, such as
    /// `Choice`, don't support or use the `End` field.
    pub end:         bool,
    /// A path that selects a portion of the state's input to be passed to the
    /// state's task for processing. If omitted, it has the value $ which
    /// designates the entire input.
    pub input_path:  Option<InputPath>,
    /// A path that selects a portion of the state's input to be passed to the
    /// state's output. If omitted, it has the value $ which designates the
    /// entire input.
    pub output_path: Option<OutputPath>,
}
