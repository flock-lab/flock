use prettytable::format;
use rustyline::completion::Pair as RustlinePair;

use crate::env::Env;
use crate::output::ScqWriter;

lazy_static! {
    static ref TBLFMT: format::TableFormat = format::FormatBuilder::new()
        .separators(
            &[format::LinePosition::Title, format::LinePosition::Bottom],
            format::LineSeparator::new('-', '+', '+', '+')
        )
        .padding(1, 1)
        .build();
}

pub trait Cmd {
    // break if returns true
    fn exec(
        &self,
        env: &mut Env,
        args: &mut dyn Iterator<Item = &str>,
        writer: &mut ScqWriter,
    ) -> bool;
    fn is(&self, l: &str) -> bool;
    fn get_name(&self) -> &'static str;
    fn try_complete(&self, index: usize, prefix: &str, env: &Env) -> Vec<RustlinePair>;
    fn complete_option(&self, prefix: &str) -> Vec<RustlinePair>;
    fn write_help(&self, writer: &mut ScqWriter);
    fn about(&self) -> &'static str;
}
