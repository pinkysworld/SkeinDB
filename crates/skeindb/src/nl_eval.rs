use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::engine::{ColumnSchema, Engine};
use skeindb_skeinql::methods::{AiNlTranslateParams, RowObject, SchemaColumnInfo};
use skeindb_skeinql::types::{BaseTableRef, Lit, Query, ResultFormat};

#[derive(Debug, Clone, Deserialize)]
struct NlEvalExample {
    pub id: Option<String>,
    pub db: String,
    pub request: String,

    #[serde(alias = "query")]
    pub expected_query: serde_json::Value,

    #[serde(default, alias = "prediction")]
    pub prediction_query: Option<serde_json::Value>,

    #[serde(default)]
    pub args: Option<Vec<Lit>>,

    #[serde(default)]
    pub fixtures: Option<NlEvalFixtures>,
}

#[derive(Debug, Clone, Deserialize)]
struct NlEvalFixtures {
    #[serde(default)]
    pub tables: Vec<NlEvalTable>,

    #[serde(default)]
    pub rows: Vec<NlEvalRowInsert>,
}

#[derive(Debug, Clone, Deserialize)]
struct NlEvalTable {
    pub db: String,
    pub table: String,
    pub columns: Vec<SchemaColumnInfo>,

    #[serde(default)]
    pub primary_key: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct NlEvalRowInsert {
    pub table: BaseTableRef,
    pub rows: Vec<RowObject>,
}

#[derive(Debug, Default, Serialize)]
pub struct NlEvalReport {
    pub total: u64,
    pub parsed_expected: u64,
    pub parsed_predicted: u64,
    pub exact_match: u64,
    pub missing_prediction: u64,
    pub parse_errors: u64,
    pub translation_errors: u64,
    pub execution_matches: u64,
    pub execution_errors: u64,
}

pub fn run_nl_eval(dataset_path: &Path, execute: bool, limit: Option<usize>) -> anyhow::Result<()> {
    let examples = parse_dataset(dataset_path)?;
    let report = eval_examples(&examples, execute, limit)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn parse_dataset(path: &Path) -> anyhow::Result<Vec<NlEvalExample>> {
    let raw = fs::read_to_string(path)?;
    let mut out = Vec::new();
    for (idx, line) in raw.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let ex: NlEvalExample =
            serde_json::from_str(trimmed).map_err(|e| anyhow::anyhow!("line {}: {e}", idx + 1))?;
        out.push(ex);
    }
    Ok(out)
}

fn eval_examples(
    examples: &[NlEvalExample],
    execute: bool,
    limit: Option<usize>,
) -> anyhow::Result<NlEvalReport> {
    let mut report = NlEvalReport::default();
    let max = limit.unwrap_or(examples.len());
    for ex in examples.iter().take(max) {
        report.total += 1;
        let dir = temp_dir("nl_eval");
        let mut engine = Engine::open(&dir)?;
        if let Some(fixtures) = ex.fixtures.as_ref() {
            apply_fixtures(&mut engine, fixtures)?;
        }

        let expected_query = match serde_json::from_value::<Query>(ex.expected_query.clone()) {
            Ok(query) => {
                report.parsed_expected += 1;
                query
            }
            Err(_) => {
                report.parse_errors += 1;
                continue;
            }
        };

        let predicted_query = match ex.prediction_query.clone() {
            Some(value) => match serde_json::from_value::<Query>(value) {
                Ok(query) => {
                    report.parsed_predicted += 1;
                    Some(query)
                }
                Err(_) => {
                    report.parse_errors += 1;
                    None
                }
            },
            None => match engine.ai_nl_translate(AiNlTranslateParams {
                db: ex.db.clone(),
                request: ex.request.clone(),
                tables: None,
                read_only: Some(true),
                include_schema: Some(true),
                max_tables: Some(16),
            }) {
                Ok(result) => {
                    if let Some(query) = result.query {
                        report.parsed_predicted += 1;
                        Some(query)
                    } else {
                        report.missing_prediction += 1;
                        None
                    }
                }
                Err(_) => {
                    report.translation_errors += 1;
                    None
                }
            },
        };

        if let Some(pred) = predicted_query.as_ref() {
            if pred == &expected_query {
                report.exact_match += 1;
            }
        }

        if execute {
            let Some(pred) = predicted_query.as_ref() else {
                std::fs::remove_dir_all(&dir).ok();
                continue;
            };
            let args = ex.args.clone().unwrap_or_default();
            let expected_data = execute_query(&engine, &expected_query, &args);
            let predicted_data = execute_query(&engine, pred, &args);
            match (expected_data, predicted_data) {
                (Ok(exp), Ok(pred)) => {
                    if exp == pred {
                        report.execution_matches += 1;
                    }
                }
                _ => report.execution_errors += 1,
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }
    Ok(report)
}

fn execute_query(
    engine: &Engine,
    query: &Query,
    args: &[Lit],
) -> anyhow::Result<serde_json::Value> {
    let result = engine.query_select(
        query,
        args,
        ResultFormat::ObjectsJson,
        false,
        None,
        None,
        None,
        false,
        None,
    )?;
    Ok(result.data.unwrap_or(serde_json::Value::Null))
}

fn apply_fixtures(engine: &mut Engine, fixtures: &NlEvalFixtures) -> anyhow::Result<()> {
    for table in fixtures.tables.iter() {
        let columns = table
            .columns
            .iter()
            .map(|col| ColumnSchema {
                name: col.name.clone(),
                r#type: col.r#type.clone(),
                nullable: col.nullable,
                auto_increment: col.auto_increment,
            })
            .collect();
        engine.create_table(
            &table.db,
            &table.table,
            columns,
            table.primary_key.clone(),
            false,
            None,
        )?;
    }
    for row in fixtures.rows.iter() {
        engine.data_insert(&row.table, row.rows.clone(), None)?;
    }
    Ok(())
}

fn temp_dir(label: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let unique = format!(
        "skeindb_{}_{}_{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );
    dir.push(unique);
    std::fs::create_dir_all(&dir).ok();
    dir
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_dataset(path: &Path, lines: &[&str]) -> anyhow::Result<()> {
        let content = lines.join("\n");
        fs::write(path, content)?;
        Ok(())
    }

    #[test]
    fn parse_dataset_skips_comments() -> anyhow::Result<()> {
        let dir = temp_dir("nl_eval_parse");
        let path = dir.join("dataset.jsonl");
        write_dataset(
            &path,
            &[
                "# comment",
                r#"{"db":"app","request":"list users","query":{"body":{"select":{"projection":[{"expr":{"col":"id"}}],"from":[{"db":"app","table":"users"}]}}}}"#,
            ],
        )?;
        let parsed = parse_dataset(&path)?;
        assert_eq!(parsed.len(), 1);
        Ok(())
    }

    #[test]
    fn eval_examples_exact_and_exec_match() -> anyhow::Result<()> {
        let examples = vec![NlEvalExample {
            id: None,
            db: "app".to_string(),
            request: "list users".to_string(),
            expected_query: serde_json::json!({
                "body": {
                    "select": {
                        "projection": [{"expr":{"col":"id"}}],
                        "from": [{"db":"app","table":"users"}]
                    }
                }
            }),
            prediction_query: Some(serde_json::json!({
                "body": {
                    "select": {
                        "projection": [{"expr":{"col":"id"}}],
                        "from": [{"db":"app","table":"users"}]
                    }
                }
            })),
            args: None,
            fixtures: Some(NlEvalFixtures {
                tables: vec![NlEvalTable {
                    db: "app".to_string(),
                    table: "users".to_string(),
                    columns: vec![SchemaColumnInfo {
                        name: "id".to_string(),
                        r#type: skeindb_skeinql::types::TypeDesc {
                            kind: "u64".to_string(),
                            max: None,
                            precision: None,
                            scale: None,
                            charset: None,
                            collation: None,
                            unsigned: None,
                        },
                        nullable: false,
                        auto_increment: false,
                    }],
                    primary_key: vec!["id".to_string()],
                }],
                rows: vec![NlEvalRowInsert {
                    table: BaseTableRef {
                        db: "app".to_string(),
                        table: "users".to_string(),
                        r#as: None,
                    },
                    rows: vec![{
                        let mut row = RowObject::new();
                        row.insert("id".to_string(), Lit::U64 { v: 1 });
                        row
                    }],
                }],
            }),
        }];

        let report = eval_examples(&examples, true, None)?;
        assert_eq!(report.total, 1);
        assert_eq!(report.exact_match, 1);
        assert_eq!(report.execution_matches, 1);
        Ok(())
    }
}
