pub(crate) mod rules;
pub mod settings;

#[cfg(test)]
mod tests {
    use std::path::Path;

    use anyhow::Result;
    use test_case::test_case;

    use crate::assert_messages;
    use crate::registry::Rule;
    use crate::settings::Settings;
    use crate::test::test_path;

    #[test_case(0)]
    #[test_case(3)]
    #[test_case(5)]
    fn max_complexity_spark(max_complexity: usize) -> Result<()> {
        let snapshot = format!("max_complexity_{max_complexity}");

        let diagnostics = test_path(
            Path::new("pyspark/PS001.py"),
            &Settings {
                pyspark: super::settings::Settings { max_complexity },
                ..Settings::for_rules(vec![Rule::SparkComplexStructure])
            },
        )?;

        assert_messages!(snapshot, diagnostics);
        Ok(())
    }
}
