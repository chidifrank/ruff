//! Settings for the `pyspark` plugin.

use crate::display_settings;
use ruff_macros::CacheKey;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, CacheKey)]
pub struct Settings {
    pub max_complexity: usize,
}

pub const DEFAULT_MAX_COMPLEXITY: usize = 3;

impl Default for Settings {
    fn default() -> Self {
        Self {
            max_complexity: DEFAULT_MAX_COMPLEXITY,
        }
    }
}

impl Display for Settings {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        display_settings! {
            formatter = f,
            namespace = "linter.pyspark",
            fields = [
                self.max_complexity
            ]
        }
        Ok(())
    }
}
