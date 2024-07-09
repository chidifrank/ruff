//! Settings for the `pyspark` plugin.

use serde::{Deserialize, Serialize};

use ruff_macros::{CacheKey, CombineOptions, ConfigurationOptions};

#[derive(
    Debug, PartialEq, Eq, Serialize, Deserialize, Default, ConfigurationOptions, CombineOptions,
)]
#[serde(
    deny_unknown_fields,
    rename_all = "kebab-case",
    rename = "PySparkOptions"
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Options {
    #[option(
        default = "3",
        value_type = "int",
        example = r#"
            # Flag errors (`PS001.py`) whenever the complexity level exceeds 3.
            max-complexity = 3
        "#
    )]
    /// The maximum PySpark transformation complexity to allow before triggering `PS001.py` errors.
    pub max_complexity: Option<usize>,
}

#[derive(Debug, CacheKey)]
pub struct Settings {
    pub max_complexity: usize,
}

const DEFAULT_MAX_COMPLEXITY: usize = 3;

impl Default for Settings {
    fn default() -> Self {
        Self {
            max_complexity: DEFAULT_MAX_COMPLEXITY,
        }
    }
}

impl From<Options> for Settings {
    fn from(options: Options) -> Self {
        Self {
            max_complexity: options.max_complexity.unwrap_or(DEFAULT_MAX_COMPLEXITY),
        }
    }
}

impl From<Settings> for Options {
    fn from(settings: Settings) -> Self {
        Self {
            max_complexity: Some(settings.max_complexity),
        }
    }
}
