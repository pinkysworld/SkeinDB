use skeindb_skeinql::types::*;

#[test]
fn expr_roundtrip_ops() {
    let e = Expr::Op {
        op: "and".to_string(),
        a: None,
        b: None,
        args: Some(vec![
            Expr::Op {
                op: "eq".to_string(),
                a: Some(Box::new(Expr::Col {
                    col: "status".to_string(),
                    table: None,
                })),
                b: Some(Box::new(Expr::Lit {
                    lit: Lit::Str {
                        v: "open".to_string(),
                    },
                })),
                args: None,
                list: None,
                lo: None,
                hi: None,
            },
            Expr::Op {
                op: "gt".to_string(),
                a: Some(Box::new(Expr::Col {
                    col: "age".to_string(),
                    table: None,
                })),
                b: Some(Box::new(Expr::Param { param: 0 })),
                args: None,
                list: None,
                lo: None,
                hi: None,
            },
        ]),
        list: None,
        lo: None,
        hi: None,
    };

    let json = serde_json::to_string(&e).unwrap();
    let back: Expr = serde_json::from_str(&json).unwrap();
    assert_eq!(e, back);
}

#[test]
fn query_roundtrip_join() {
    let q = Query {
        with: vec![],
        body: Box::new(QueryBody::Select {
            select: Box::new(SelectBody {
                distinct: Some(false),
                projection: vec![SelectItem {
                    expr: Expr::Col {
                        col: "id".to_string(),
                        table: Some("u".to_string()),
                    },
                    r#as: Some("id".to_string()),
                }],
                from: Some(vec![TableRef::Join(JoinTableRef {
                    join: JoinRef {
                        join_type: JoinType::Inner,
                        left: Box::new(TableRef::Base(BaseTableRef {
                            db: "mydb".to_string(),
                            table: "users".to_string(),
                            r#as: Some("u".to_string()),
                        })),
                        right: Box::new(TableRef::Base(BaseTableRef {
                            db: "mydb".to_string(),
                            table: "orders".to_string(),
                            r#as: Some("o".to_string()),
                        })),
                        on: Some(Expr::Op {
                            op: "eq".to_string(),
                            a: Some(Box::new(Expr::Col {
                                col: "id".to_string(),
                                table: Some("u".to_string()),
                            })),
                            b: Some(Box::new(Expr::Col {
                                col: "user_id".to_string(),
                                table: Some("o".to_string()),
                            })),
                            args: None,
                            list: None,
                            lo: None,
                            hi: None,
                        }),
                    },
                })]),
                r#where: None,
                group_by: None,
                having: None,
            }),
        }),
        order_by: vec![],
        limit: Some(LimitClause {
            limit: Some(10),
            offset: Some(0),
        }),
        lock: Some(LockMode::None),
    };

    let json = serde_json::to_string_pretty(&q).unwrap();
    let back: Query = serde_json::from_str(&json).unwrap();
    assert_eq!(q, back);
}

#[test]
fn query_roundtrip_subquery_and_setop() {
    let subquery = Query {
        with: vec![],
        body: Box::new(QueryBody::Select {
            select: Box::new(SelectBody {
                distinct: None,
                projection: vec![SelectItem {
                    expr: Expr::Col {
                        col: "score".to_string(),
                        table: Some("s".to_string()),
                    },
                    r#as: None,
                }],
                from: Some(vec![TableRef::Base(BaseTableRef {
                    db: "app".to_string(),
                    table: "scores".to_string(),
                    r#as: Some("s".to_string()),
                })]),
                r#where: Some(Expr::Op {
                    op: "gt".to_string(),
                    a: Some(Box::new(Expr::Col {
                        col: "score".to_string(),
                        table: Some("s".to_string()),
                    })),
                    b: Some(Box::new(Expr::Lit {
                        lit: Lit::U64 { v: 10 },
                    })),
                    args: None,
                    list: None,
                    lo: None,
                    hi: None,
                }),
                group_by: None,
                having: None,
            }),
        }),
        order_by: vec![OrderBy {
            expr: Expr::Col {
                col: "score".to_string(),
                table: Some("s".to_string()),
            },
            dir: Some(OrderDir::Desc),
        }],
        limit: Some(LimitClause {
            limit: Some(1),
            offset: Some(0),
        }),
        lock: None,
    };

    let left_select = SelectBody {
        distinct: Some(true),
        projection: vec![
            SelectItem {
                expr: Expr::Col {
                    col: "id".to_string(),
                    table: Some("u".to_string()),
                },
                r#as: None,
            },
            SelectItem {
                expr: Expr::Subquery {
                    subquery: SubqueryExpr {
                        query: subquery.clone(),
                    },
                },
                r#as: Some("latest_score".to_string()),
            },
        ],
        from: Some(vec![TableRef::Base(BaseTableRef {
            db: "app".to_string(),
            table: "users".to_string(),
            r#as: Some("u".to_string()),
        })]),
        r#where: Some(Expr::Op {
            op: "eq".to_string(),
            a: Some(Box::new(Expr::Col {
                col: "active".to_string(),
                table: Some("u".to_string()),
            })),
            b: Some(Box::new(Expr::Lit {
                lit: Lit::Bool { v: true },
            })),
            args: None,
            list: None,
            lo: None,
            hi: None,
        }),
        group_by: Some(vec![Expr::Col {
            col: "id".to_string(),
            table: Some("u".to_string()),
        }]),
        having: Some(Expr::Op {
            op: "gt".to_string(),
            a: Some(Box::new(Expr::Func {
                name: "count".to_string(),
                args: vec![Expr::Col {
                    col: "id".to_string(),
                    table: Some("u".to_string()),
                }],
                distinct: Some(false),
            })),
            b: Some(Box::new(Expr::Lit {
                lit: Lit::U64 { v: 1 },
            })),
            args: None,
            list: None,
            lo: None,
            hi: None,
        }),
    };

    let right_select = SelectBody {
        distinct: Some(false),
        projection: vec![SelectItem {
            expr: Expr::Col {
                col: "id".to_string(),
                table: Some("r".to_string()),
            },
            r#as: None,
        }],
        from: Some(vec![TableRef::Subquery(SubqueryTableRef {
            subquery: SubqueryRef {
                query: subquery.clone(),
                r#as: "r".to_string(),
            },
        })]),
        r#where: Some(Expr::Exists {
            exists: ExistsExpr {
                query: subquery.clone(),
                negated: Some(true),
            },
        }),
        group_by: None,
        having: None,
    };

    let q = Query {
        with: vec![Cte {
            name: "recent_scores".to_string(),
            query: subquery.clone(),
        }],
        body: Box::new(QueryBody::Setop {
            setop: SetOp {
                kind: SetOpKind::Union,
                all: true,
                left: Box::new(QueryBody::Select {
                    select: Box::new(left_select),
                }),
                right: Box::new(QueryBody::Select {
                    select: Box::new(right_select),
                }),
            },
        }),
        order_by: vec![OrderBy {
            expr: Expr::Col {
                col: "id".to_string(),
                table: None,
            },
            dir: Some(OrderDir::Desc),
        }],
        limit: None,
        lock: Some(LockMode::None),
    };

    let json = serde_json::to_string_pretty(&q).unwrap();
    let back: Query = serde_json::from_str(&json).unwrap();
    assert_eq!(q, back);
}

#[test]
fn embedding_literal_roundtrip() {
    let lit = Lit::Embedding {
        dims: 3,
        v: vec![0.1, 0.2, 0.3],
        model: Some("text-embed-v1".to_string()),
    };
    let json = serde_json::to_string(&lit).unwrap();
    let back: Lit = serde_json::from_str(&json).unwrap();
    assert_eq!(lit, back);
}
