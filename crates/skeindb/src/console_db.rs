use serde::{Deserialize, Serialize};
use sqlparser::{
    ast::{
        BinaryOperator, ColumnDef as AstColumnDef, ColumnOption, DataType, Expr, Ident, ObjectName,
        Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, UnaryOperator,
        Value as AstValue,
    },
    dialect::MySqlDialect,
    parser::Parser,
};
use std::collections::HashMap;
use thiserror::Error;

const SAMPLE_SQL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../samples/sample.sql"
));

#[derive(Debug, Error)]
pub enum SqlError {
    #[error("sql parse error: {0}")]
    Parse(String),

    #[error("unsupported statement: {0}")]
    Unsupported(String),

    #[error("unknown table: {0}")]
    UnknownTable(String),

    #[error("unknown column: {0}")]
    UnknownColumn(String),

    #[error("invalid value: {0}")]
    InvalidValue(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub row_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SqlResult {
    pub columns: Vec<ColumnSchema>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub affected_rows: u64,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SqlExecResponse {
    pub results: Vec<SqlResult>,
}

#[derive(Debug, Clone)]
pub struct ConsoleDb {
    tables: HashMap<String, Table>,
}

#[derive(Debug, Clone)]
struct Table {
    name: String,
    columns: Vec<ColumnSchema>,
    column_index: HashMap<String, usize>,
    rows: Vec<Vec<CellValue>>,
}

#[derive(Debug, Clone, PartialEq)]
enum CellValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
}

impl ConsoleDb {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn schema(&self) -> Vec<TableSchema> {
        let mut tables: Vec<TableSchema> = self
            .tables
            .values()
            .map(|table| TableSchema {
                name: table.name.clone(),
                columns: table.columns.clone(),
                row_count: table.rows.len(),
            })
            .collect();
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        tables
    }

    pub fn select_table(&self, name: &str, limit: usize) -> Result<SqlResult, SqlError> {
        let table = self.table(name)?;
        let mut rows = Vec::new();
        for row in table.rows.iter().take(limit) {
            rows.push(row.iter().map(CellValue::to_json).collect());
        }
        let row_count = rows.len() as u64;
        Ok(SqlResult {
            columns: table.columns.clone(),
            rows,
            affected_rows: row_count,
            message: None,
        })
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<SqlExecResponse, SqlError> {
        let dialect = MySqlDialect {};
        let statements =
            Parser::parse_sql(&dialect, sql).map_err(|err| SqlError::Parse(err.to_string()))?;
        let mut results = Vec::new();
        for statement in statements {
            let result = self.execute_statement(statement)?;
            results.push(result);
        }
        Ok(SqlExecResponse { results })
    }

    pub fn load_sample(&mut self) -> Result<SqlExecResponse, SqlError> {
        self.execute_sql(SAMPLE_SQL)
    }

    pub fn sample_sql() -> &'static str {
        SAMPLE_SQL
    }

    fn execute_statement(&mut self, statement: Statement) -> Result<SqlResult, SqlError> {
        match statement {
            Statement::CreateTable {
                name,
                columns,
                if_not_exists,
                ..
            } => self.create_table(name, columns, if_not_exists),
            Statement::Drop {
                object_type, names, ..
            } => {
                if object_type != sqlparser::ast::ObjectType::Table {
                    return Err(SqlError::Unsupported(format!(
                        "drop {} not supported",
                        object_type
                    )));
                }
                let mut affected = 0;
                for name in names {
                    let normalized = normalize_ident(&object_name_to_string(&name));
                    if self.tables.remove(&normalized).is_some() {
                        affected += 1;
                    }
                }
                Ok(SqlResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    affected_rows: affected,
                    message: Some(format!("dropped {} table(s)", affected)),
                })
            }
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                let source = source.ok_or_else(|| {
                    SqlError::Unsupported("INSERT without VALUES is not supported".to_string())
                })?;
                self.insert(table_name, columns, *source)
            }
            Statement::Query(query) => self.query(*query),
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => self.update(table, assignments, selection),
            statement @ Statement::Delete { .. } => self.delete(statement),
            Statement::ShowTables { .. } => self.show_tables(),
            _ => Err(SqlError::Unsupported(format!("{statement}"))),
        }
    }

    fn create_table(
        &mut self,
        name: ObjectName,
        columns: Vec<AstColumnDef>,
        if_not_exists: bool,
    ) -> Result<SqlResult, SqlError> {
        let table_name = object_name_to_string(&name);
        let normalized = normalize_ident(&table_name);
        if self.tables.contains_key(&normalized) {
            if if_not_exists {
                return Ok(SqlResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    affected_rows: 0,
                    message: Some(format!("table {table_name} already exists")),
                });
            }
            return Err(SqlError::Unsupported(format!(
                "table {table_name} already exists"
            )));
        }

        let mut column_index = HashMap::new();
        let mut table_columns = Vec::new();
        for (idx, column) in columns.iter().enumerate() {
            let mut nullable = true;
            for option in &column.options {
                match option.option {
                    ColumnOption::NotNull => nullable = false,
                    ColumnOption::Null => nullable = true,
                    _ => {}
                }
            }
            let col_name = column.name.value.clone();
            column_index.insert(normalize_ident(&col_name), idx);
            table_columns.push(ColumnSchema {
                name: col_name,
                data_type: data_type_to_string(&column.data_type),
                nullable,
            });
        }

        self.tables.insert(
            normalized,
            Table {
                name: table_name,
                columns: table_columns,
                column_index,
                rows: Vec::new(),
            },
        );

        Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            message: Some("table created".to_string()),
        })
    }

    fn insert(
        &mut self,
        table_name: ObjectName,
        columns: Vec<Ident>,
        source: Query,
    ) -> Result<SqlResult, SqlError> {
        let table_key = object_name_to_string(&table_name);
        let table = self.table_mut(&table_key)?;
        let values = extract_values(&source)?;

        let target_columns: Vec<usize> = if columns.is_empty() {
            (0..table.columns.len()).collect()
        } else {
            columns
                .iter()
                .map(|ident| table.column_idx(&ident.value))
                .collect::<Result<Vec<_>, _>>()?
        };

        let expected = target_columns.len();
        let mut inserted = 0u64;
        for row in values {
            if row.len() != expected {
                return Err(SqlError::InvalidValue(format!(
                    "expected {expected} values, got {}",
                    row.len()
                )));
            }
            let mut new_row = vec![CellValue::Null; table.columns.len()];
            for (idx, expr) in row.iter().enumerate() {
                let col_idx = target_columns[idx];
                new_row[col_idx] = expr_to_literal(expr)?;
            }
            table.rows.push(new_row);
            inserted += 1;
        }

        Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: inserted,
            message: Some(format!("inserted {inserted} row(s)")),
        })
    }

    fn query(&self, query: Query) -> Result<SqlResult, SqlError> {
        match *query.body {
            SetExpr::Select(select) => self.select(*select, query.limit),
            _ => Err(SqlError::Unsupported(
                "only SELECT queries are supported".to_string(),
            )),
        }
    }

    fn select(&self, select: Select, limit: Option<Expr>) -> Result<SqlResult, SqlError> {
        let table = extract_table(&select.from)?;
        let table = self.table(&table)?;

        let mut selected_columns = Vec::new();
        let mut column_indexes = Vec::new();
        if select.projection.is_empty()
            || select
                .projection
                .iter()
                .any(|item| matches!(item, SelectItem::Wildcard(_)))
        {
            selected_columns = table.columns.clone();
            column_indexes = (0..table.columns.len()).collect();
        } else {
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let (name, idx) = resolve_projection(expr, table)?;
                        let mut col = table.columns[idx].clone();
                        col.name = name;
                        selected_columns.push(col);
                        column_indexes.push(idx);
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let (_, idx) = resolve_projection(expr, table)?;
                        let mut col = table.columns[idx].clone();
                        col.name = alias.value.clone();
                        selected_columns.push(col);
                        column_indexes.push(idx);
                    }
                    SelectItem::QualifiedWildcard(_, _) => {
                        selected_columns = table.columns.clone();
                        column_indexes = (0..table.columns.len()).collect();
                        break;
                    }
                    _ => {
                        return Err(SqlError::Unsupported(
                            "only column projections are supported".to_string(),
                        ));
                    }
                }
            }
        }

        let limit = match limit {
            Some(expr) => Some(parse_limit(&expr)?),
            None => None,
        };

        let mut rows = Vec::new();
        for row in &table.rows {
            if let Some(selection) = &select.selection {
                if !eval_predicate(selection, row, &table.column_index)? {
                    continue;
                }
            }
            let mut out = Vec::new();
            for idx in &column_indexes {
                out.push(row[*idx].to_json());
            }
            rows.push(out);
            if let Some(limit) = limit {
                if rows.len() >= limit {
                    break;
                }
            }
        }

        let row_count = rows.len() as u64;
        Ok(SqlResult {
            columns: selected_columns,
            rows,
            affected_rows: row_count,
            message: None,
        })
    }

    fn update(
        &mut self,
        table: TableWithJoins,
        assignments: Vec<sqlparser::ast::Assignment>,
        selection: Option<Expr>,
    ) -> Result<SqlResult, SqlError> {
        let table_name = extract_table(&[table])?;
        let table = self.table_mut(&table_name)?;
        let column_index = table.column_index.clone();
        let mut updated = 0u64;

        for row in &mut table.rows {
            if let Some(selection) = &selection {
                if !eval_predicate(selection, row, &column_index)? {
                    continue;
                }
            }
            for assign in &assignments {
                let column_name = assignment_column_name(assign)?;
                let idx = column_idx(&column_index, &column_name)?;
                row[idx] = expr_to_literal(&assign.value)?;
            }
            updated += 1;
        }

        Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: updated,
            message: Some(format!("updated {updated} row(s)")),
        })
    }

    fn delete(&mut self, statement: Statement) -> Result<SqlResult, SqlError> {
        let (table_name, selection) = extract_delete(statement)?;
        let table = self.table_mut(&table_name)?;
        let column_index = table.column_index.clone();
        let mut deleted = 0usize;
        let rows = std::mem::take(&mut table.rows);
        let mut remaining = Vec::with_capacity(rows.len());
        for row in rows {
            let should_delete = if let Some(selection) = &selection {
                eval_predicate(selection, &row, &column_index)?
            } else {
                true
            };
            if should_delete {
                deleted += 1;
            } else {
                remaining.push(row);
            }
        }
        table.rows = remaining;

        Ok(SqlResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: deleted as u64,
            message: Some(format!("deleted {deleted} row(s)")),
        })
    }

    fn show_tables(&self) -> Result<SqlResult, SqlError> {
        let mut rows = Vec::new();
        let mut names: Vec<_> = self.tables.values().map(|t| t.name.clone()).collect();
        names.sort();
        for name in names {
            rows.push(vec![serde_json::Value::String(name)]);
        }
        Ok(SqlResult {
            columns: vec![ColumnSchema {
                name: "table_name".to_string(),
                data_type: "text".to_string(),
                nullable: false,
            }],
            rows,
            affected_rows: 0,
            message: None,
        })
    }

    fn table(&self, name: &str) -> Result<&Table, SqlError> {
        let normalized = normalize_ident(name);
        self.tables
            .get(&normalized)
            .ok_or_else(|| SqlError::UnknownTable(name.to_string()))
    }

    fn table_mut(&mut self, name: &str) -> Result<&mut Table, SqlError> {
        let normalized = normalize_ident(name);
        self.tables
            .get_mut(&normalized)
            .ok_or_else(|| SqlError::UnknownTable(name.to_string()))
    }
}

impl Default for ConsoleDb {
    fn default() -> Self {
        Self::new()
    }
}

impl Table {
    fn column_idx(&self, name: &str) -> Result<usize, SqlError> {
        let normalized = normalize_ident(name);
        self.column_index
            .get(&normalized)
            .copied()
            .ok_or_else(|| SqlError::UnknownColumn(name.to_string()))
    }
}

impl CellValue {
    fn to_json(&self) -> serde_json::Value {
        match self {
            CellValue::Null => serde_json::Value::Null,
            CellValue::Bool(v) => serde_json::Value::Bool(*v),
            CellValue::I64(v) => serde_json::Value::Number((*v).into()),
            CellValue::F64(v) => serde_json::Number::from_f64(*v)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            CellValue::String(v) => serde_json::Value::String(v.clone()),
        }
    }
}

fn normalize_ident(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

fn column_idx(column_index: &HashMap<String, usize>, name: &str) -> Result<usize, SqlError> {
    let normalized = normalize_ident(name);
    column_index
        .get(&normalized)
        .copied()
        .ok_or_else(|| SqlError::UnknownColumn(name.to_string()))
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .last()
        .map(|ident| ident.value.clone())
        .unwrap_or_default()
}

fn data_type_to_string(data_type: &DataType) -> String {
    data_type.to_string().to_ascii_lowercase()
}

fn extract_values(query: &Query) -> Result<Vec<Vec<Expr>>, SqlError> {
    match query.body.as_ref() {
        SetExpr::Values(values) => Ok(values.rows.clone()),
        _ => Err(SqlError::Unsupported(
            "only VALUES inserts are supported".to_string(),
        )),
    }
}

fn extract_table(from: &[TableWithJoins]) -> Result<String, SqlError> {
    if from.len() != 1 {
        return Err(SqlError::Unsupported(
            "only single-table queries are supported".to_string(),
        ));
    }
    let table = &from[0];
    if !table.joins.is_empty() {
        return Err(SqlError::Unsupported(
            "joins are not supported yet".to_string(),
        ));
    }
    match &table.relation {
        TableFactor::Table { name, .. } => Ok(object_name_to_string(name)),
        _ => Err(SqlError::Unsupported(
            "only table relations are supported".to_string(),
        )),
    }
}

fn resolve_projection(expr: &Expr, table: &Table) -> Result<(String, usize), SqlError> {
    match expr {
        Expr::Identifier(ident) => {
            let idx = table.column_idx(&ident.value)?;
            Ok((ident.value.clone(), idx))
        }
        Expr::CompoundIdentifier(parts) => {
            let name = parts
                .last()
                .map(|ident| ident.value.clone())
                .unwrap_or_default();
            let idx = table.column_idx(&name)?;
            Ok((name, idx))
        }
        _ => Err(SqlError::Unsupported(
            "only column projections are supported".to_string(),
        )),
    }
}

fn parse_limit(expr: &Expr) -> Result<usize, SqlError> {
    match expr {
        Expr::Value(AstValue::Number(num, _)) => num
            .parse::<usize>()
            .map_err(|_| SqlError::InvalidValue("invalid LIMIT value".to_string())),
        _ => Err(SqlError::InvalidValue(
            "only numeric LIMIT values are supported".to_string(),
        )),
    }
}

fn expr_to_literal(expr: &Expr) -> Result<CellValue, SqlError> {
    match expr {
        Expr::Value(value) => parse_ast_value(value),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(AstValue::Number(num, _)) => parse_number(&format!("-{num}")),
            _ => Err(SqlError::InvalidValue(
                "only numeric unary minus values are supported".to_string(),
            )),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(AstValue::Number(num, _)) => parse_number(num),
            _ => Err(SqlError::InvalidValue(
                "only numeric unary plus values are supported".to_string(),
            )),
        },
        _ => Err(SqlError::Unsupported(
            "only literal values are supported".to_string(),
        )),
    }
}

fn parse_ast_value(value: &AstValue) -> Result<CellValue, SqlError> {
    match value {
        AstValue::Number(num, _) => parse_number(num),
        AstValue::SingleQuotedString(s) => Ok(CellValue::String(s.clone())),
        AstValue::Boolean(v) => Ok(CellValue::Bool(*v)),
        AstValue::Null => Ok(CellValue::Null),
        _ => Err(SqlError::InvalidValue(format!(
            "unsupported literal: {value}"
        ))),
    }
}

fn parse_number(num: &str) -> Result<CellValue, SqlError> {
    if num.contains('.') || num.contains('e') || num.contains('E') {
        num.parse::<f64>()
            .map(CellValue::F64)
            .map_err(|_| SqlError::InvalidValue(format!("invalid number {num}")))
    } else {
        num.parse::<i64>()
            .map(CellValue::I64)
            .map_err(|_| SqlError::InvalidValue(format!("invalid number {num}")))
    }
}

fn eval_predicate(
    expr: &Expr,
    row: &[CellValue],
    column_index: &HashMap<String, usize>,
) -> Result<bool, SqlError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(eval_predicate(left, row, column_index)?
                && eval_predicate(right, row, column_index)?),
            BinaryOperator::Or => Ok(eval_predicate(left, row, column_index)?
                || eval_predicate(right, row, column_index)?),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq => {
                let left_val = eval_value(left, row, column_index)?;
                let right_val = eval_value(right, row, column_index)?;
                compare_values(&left_val, &right_val, op)
            }
            _ => Err(SqlError::Unsupported(
                "unsupported binary operator in WHERE".to_string(),
            )),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Ok(!eval_predicate(expr, row, column_index)?),
        Expr::Nested(expr) => eval_predicate(expr, row, column_index),
        _ => Err(SqlError::Unsupported(
            "unsupported WHERE predicate".to_string(),
        )),
    }
}

fn eval_value(
    expr: &Expr,
    row: &[CellValue],
    column_index: &HashMap<String, usize>,
) -> Result<CellValue, SqlError> {
    match expr {
        Expr::Identifier(ident) => {
            let idx = column_idx(column_index, &ident.value)?;
            Ok(row[idx].clone())
        }
        Expr::CompoundIdentifier(parts) => {
            let name = parts
                .last()
                .map(|ident| ident.value.clone())
                .unwrap_or_default();
            let idx = column_idx(column_index, &name)?;
            Ok(row[idx].clone())
        }
        Expr::Value(_) | Expr::UnaryOp { .. } => expr_to_literal(expr),
        Expr::Nested(expr) => eval_value(expr, row, column_index),
        _ => Err(SqlError::Unsupported(
            "unsupported expression in WHERE".to_string(),
        )),
    }
}

fn compare_values(
    left: &CellValue,
    right: &CellValue,
    op: &BinaryOperator,
) -> Result<bool, SqlError> {
    if matches!(left, CellValue::Null) || matches!(right, CellValue::Null) {
        return Ok(false);
    }
    match (left, right) {
        (CellValue::I64(a), CellValue::I64(b)) => compare_numbers(*a as f64, *b as f64, op),
        (CellValue::F64(a), CellValue::F64(b)) => compare_numbers(*a, *b, op),
        (CellValue::I64(a), CellValue::F64(b)) => compare_numbers(*a as f64, *b, op),
        (CellValue::F64(a), CellValue::I64(b)) => compare_numbers(*a, *b as f64, op),
        (CellValue::String(a), CellValue::String(b)) => compare_ordering(a.cmp(b), op),
        (CellValue::Bool(a), CellValue::Bool(b)) => compare_ordering(a.cmp(b), op),
        _ => Err(SqlError::InvalidValue(
            "type mismatch in comparison".to_string(),
        )),
    }
}

fn compare_numbers(left: f64, right: f64, op: &BinaryOperator) -> Result<bool, SqlError> {
    let ordering = left
        .partial_cmp(&right)
        .ok_or_else(|| SqlError::InvalidValue("invalid numeric comparison".to_string()))?;
    compare_ordering(ordering, op)
}

fn compare_ordering(ordering: std::cmp::Ordering, op: &BinaryOperator) -> Result<bool, SqlError> {
    match op {
        BinaryOperator::Eq => Ok(ordering == std::cmp::Ordering::Equal),
        BinaryOperator::NotEq => Ok(ordering != std::cmp::Ordering::Equal),
        BinaryOperator::Gt => Ok(ordering == std::cmp::Ordering::Greater),
        BinaryOperator::Lt => Ok(ordering == std::cmp::Ordering::Less),
        BinaryOperator::GtEq => Ok(ordering != std::cmp::Ordering::Less),
        BinaryOperator::LtEq => Ok(ordering != std::cmp::Ordering::Greater),
        _ => Err(SqlError::Unsupported(
            "unsupported comparison operator".to_string(),
        )),
    }
}

fn assignment_column_name(assign: &sqlparser::ast::Assignment) -> Result<String, SqlError> {
    if let Some(ident) = assign.id.last() {
        Ok(ident.value.clone())
    } else {
        Err(SqlError::InvalidValue(
            "assignment column missing".to_string(),
        ))
    }
}

fn extract_delete(statement: Statement) -> Result<(String, Option<Expr>), SqlError> {
    match statement {
        Statement::Delete {
            from, selection, ..
        } => {
            let table = extract_table(&from)?;
            Ok((table, selection))
        }
        _ => Err(SqlError::Unsupported(
            "only DELETE statements are supported".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_insert_select_roundtrip() {
        let mut db = ConsoleDb::new();
        let sql = "CREATE TABLE users (id INT, name TEXT); \
            INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Linus'); \
            SELECT id, name FROM users WHERE id = 1;";
        let response = db.execute_sql(sql).unwrap();
        assert_eq!(response.results.len(), 3);
        let select = response.results.last().unwrap();
        assert_eq!(select.columns.len(), 2);
        assert_eq!(select.rows.len(), 1);
        assert_eq!(
            select.rows[0],
            vec![serde_json::Value::from(1), serde_json::Value::from("Ada")]
        );
    }

    #[test]
    fn update_and_delete() {
        let mut db = ConsoleDb::new();
        db.execute_sql(
            "CREATE TABLE tasks (id INT, done BOOL); \
            INSERT INTO tasks (id, done) VALUES (1, true), (2, false); \
            UPDATE tasks SET done = true WHERE id = 2; \
            DELETE FROM tasks WHERE id = 1;",
        )
        .unwrap();
        let response = db
            .execute_sql("SELECT id, done FROM tasks WHERE id = 2;")
            .unwrap();
        let select = response.results.last().unwrap();
        assert_eq!(select.rows.len(), 1);
        assert_eq!(
            select.rows[0],
            vec![serde_json::Value::from(2), serde_json::Value::from(true)]
        );
    }

    #[test]
    fn load_sample_sql() {
        let mut db = ConsoleDb::new();
        db.load_sample().unwrap();
        let tables = db.schema();
        assert!(tables.iter().any(|table| table.name == "users"));
        assert!(tables.iter().any(|table| table.name == "projects"));
    }
}
