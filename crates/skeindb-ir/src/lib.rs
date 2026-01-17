//! SkeinIR v1 - Rust types (scaffold)
//!
//! This crate defines the canonical intermediate representation used by SkeinDB.
//! See: docs/SKEINIR.md

use serde::{Deserialize, Serialize};

pub type DbId = u32;
pub type TableId = u32;
pub type ColId = u32;
pub type IndexId = u32;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Ident {
    pub original: String,
    pub normalized: String,
}

impl Ident {
    pub fn new<S: Into<String>>(s: S) -> Self {
        let original = s.into();
        let normalized = original.to_lowercase();
        Self {
            original,
            normalized,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectName {
    pub parts: Vec<Ident>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    Decimal { precision: u16, scale: u16 },
    Varchar { len: u32 },
    Text,
    Longtext,
    Binary { len: u32 },
    Varbinary { len: u32 },
    Blob,
    Longblob,
    Json,
    Date,
    Time { fsp: u8 },
    Datetime { fsp: u8 },
    Timestamp { fsp: u8 },
    Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnType {
    pub ty: DataType,
    pub nullable: bool,
    pub charset: Option<String>,
    pub collation: Option<String>,
    pub unsigned: bool,
    pub zerofill: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub col_id: ColId,
    pub name: Ident,
    pub col_type: ColumnType,
    pub default_expr: Option<Expr>,
    pub auto_increment: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Decimal(String),
    String(String),
    Bytes(Vec<u8>),
    Json(String),
    Date { y: u16, m: u8, d: u8 },
    Time { micros: i64 },
    Datetime { micros: i64 },
    Timestamp { micros: i64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UnaryOp {
    Not,
    Negate,
    BitNot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    BitAnd,
    BitOr,
    BitXor,
    Shl,
    Shr,
    Concat,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnRef {
    pub table_alias: Option<Ident>,
    pub col_name: Ident,
    pub col_id: Option<ColId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Lit(Literal),
    Col(ColumnRef),
    Param {
        index: u32,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Func {
        name: Ident,
        args: Vec<Expr>,
        distinct: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ident_roundtrip_json() {
        let id = Ident::new("Wp_Options");
        let json = serde_json::to_string(&id).unwrap();
        let back: Ident = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
        assert_eq!(back.normalized, "wp_options");
    }

    #[test]
    fn expr_roundtrip_json() {
        let expr = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Col(ColumnRef {
                table_alias: None,
                col_name: Ident::new("option_name"),
                col_id: None,
            })),
            right: Box::new(Expr::Lit(Literal::String("siteurl".to_string()))),
        };
        let json = serde_json::to_string(&expr).unwrap();
        let back: Expr = serde_json::from_str(&json).unwrap();
        assert_eq!(expr, back);
    }
}
