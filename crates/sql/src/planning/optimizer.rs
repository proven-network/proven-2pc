//! Query optimizer for SQL execution plans - simplified version

use super::plan::{Node, Plan};
use crate::error::Result;
use crate::types::expression::Expression;
use crate::types::schema::Table;
use crate::types::value::Value;
use std::collections::HashMap;

/// Query optimizer that applies transformation rules to execution plans
pub struct Optimizer {
    /// Schema information for tables
    #[allow(dead_code)]
    schemas: HashMap<String, Table>,
}

impl Optimizer {
    /// Create a new optimizer
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        Self { schemas }
    }

    /// Optimize a query plan
    pub fn optimize(&self, plan: Plan) -> Result<Plan> {
        match plan {
            Plan::Select(node) => {
                let optimized = self.optimize_node(*node)?;
                Ok(Plan::Select(Box::new(optimized)))
            }
            other => Ok(other), // Don't optimize non-SELECT statements for now
        }
    }

    /// Optimize a plan node recursively
    fn optimize_node(&self, node: Node) -> Result<Node> {
        // Apply optimizations bottom-up
        let node = self.optimize_children(node)?;

        // Apply simple optimizations
        let node = self.simplify_constants(node)?;
        let node = self.remove_redundant(node)?;

        Ok(node)
    }

    /// Recursively optimize child nodes
    fn optimize_children(&self, node: Node) -> Result<Node> {
        Ok(match node {
            Node::Filter { source, predicate } => {
                // Try to push filter down to scan and convert to index scan
                if let Node::Scan { table, alias } = source.as_ref()
                    && let Some(index_scan) =
                        self.try_convert_to_index_scan(table.clone(), alias.clone(), &predicate)
                {
                    return Ok(index_scan);
                }
                Node::Filter {
                    source: Box::new(self.optimize_node(*source)?),
                    predicate,
                }
            }
            Node::Projection {
                source,
                expressions,
                aliases,
            } => Node::Projection {
                source: Box::new(self.optimize_node(*source)?),
                expressions,
                aliases,
            },
            Node::Order { source, order_by } => Node::Order {
                source: Box::new(self.optimize_node(*source)?),
                order_by,
            },
            Node::Limit { source, limit } => Node::Limit {
                source: Box::new(self.optimize_node(*source)?),
                limit,
            },
            Node::Offset { source, offset } => Node::Offset {
                source: Box::new(self.optimize_node(*source)?),
                offset,
            },
            Node::Aggregate {
                source,
                group_by,
                aggregates,
            } => Node::Aggregate {
                source: Box::new(self.optimize_node(*source)?),
                group_by,
                aggregates,
            },
            Node::HashJoin {
                left,
                right,
                left_col,
                right_col,
                join_type,
            } => Node::HashJoin {
                left: Box::new(self.optimize_node(*left)?),
                right: Box::new(self.optimize_node(*right)?),
                left_col,
                right_col,
                join_type,
            },
            Node::NestedLoopJoin {
                left,
                right,
                predicate,
                join_type,
            } => Node::NestedLoopJoin {
                left: Box::new(self.optimize_node(*left)?),
                right: Box::new(self.optimize_node(*right)?),
                predicate,
                join_type,
            },
            Node::IndexScan { .. } => node,      // Already optimized
            Node::IndexRangeScan { .. } => node, // Already optimized
            other => other,                      // Leaf nodes
        })
    }

    /// Try to convert a filter on a scan to an index scan
    fn try_convert_to_index_scan(
        &self,
        table: String,
        alias: Option<String>,
        predicate: &Expression,
    ) -> Option<Node> {
        // Look for simple equality predicates like column = value
        if let Expression::Equal(left, right) = predicate {
            // Check if left is a column and right is a constant
            if let (Expression::Column(col_idx), value_expr) = (&**left, &**right)
                && let Some(schema) = self.schemas.get(&table)
                && *col_idx < schema.columns.len()
            {
                let column = &schema.columns[*col_idx];
                // Check if this column actually has an index
                if (column.index || column.primary_key) && Self::is_constant_expr(value_expr) {
                    return Some(Node::IndexScan {
                        table,
                        alias,
                        index_name: column.name.clone(),
                        values: vec![value_expr.clone()],
                    });
                }
            }
            // Also check if right is column and left is constant (commutative)
            if let (value_expr, Expression::Column(col_idx)) = (&**left, &**right)
                && let Some(schema) = self.schemas.get(&table)
                && *col_idx < schema.columns.len()
            {
                let column = &schema.columns[*col_idx];
                if (column.index || column.primary_key) && Self::is_constant_expr(value_expr) {
                    return Some(Node::IndexScan {
                        table,
                        alias,
                        index_name: column.name.clone(),
                        values: vec![value_expr.clone()],
                    });
                }
            }
        }
        None
    }

    /// Check if an expression is constant (doesn't depend on row data)
    fn is_constant_expr(expr: &Expression) -> bool {
        match expr {
            Expression::Constant(_) => true,
            Expression::Function(name, _) if name == "now" || name == "uuid" => true,
            _ => false,
        }
    }

    /// Simplify constant expressions where possible
    fn simplify_constants(&self, node: Node) -> Result<Node> {
        Ok(match node {
            Node::Filter { source, predicate } => {
                // Check for constant true/false predicates
                if let Expression::Constant(Value::Bool(false)) = predicate {
                    // Filter is always false - return empty
                    return Ok(Node::Nothing);
                }
                if let Expression::Constant(Value::Bool(true)) = predicate {
                    // Filter is always true - remove it
                    return Ok(*source);
                }

                Node::Filter { source, predicate }
            }
            other => other,
        })
    }

    /// Remove redundant nodes
    fn remove_redundant(&self, node: Node) -> Result<Node> {
        Ok(match node {
            // Remove LIMIT 0
            Node::Limit { limit: 0, .. } => Node::Nothing,

            // Remove OFFSET 0
            Node::Offset { source, offset: 0 } => *source,

            // Combine adjacent filters
            Node::Filter {
                source,
                predicate: pred1,
            } => {
                if let Node::Filter {
                    source: inner_source,
                    predicate: pred2,
                } = *source
                {
                    // Combine predicates with AND
                    Node::Filter {
                        source: inner_source,
                        predicate: Expression::And(Box::new(pred1), Box::new(pred2)),
                    }
                } else {
                    Node::Filter {
                        source,
                        predicate: pred1,
                    }
                }
            }

            other => other,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::data_type::DataType;

    fn test_schemas() -> HashMap<String, Table> {
        let mut schemas = HashMap::new();
        schemas.insert(
            "users".to_string(),
            Table::new(
                "users".to_string(),
                vec![
                    crate::types::schema::Column::new("id".to_string(), DataType::I64)
                        .primary_key(),
                    crate::types::schema::Column::new("name".to_string(), DataType::Str),
                    crate::types::schema::Column::new("age".to_string(), DataType::I64),
                ],
            )
            .unwrap(),
        );
        schemas
    }

    #[test]
    fn test_constant_false_filter() {
        let optimizer = Optimizer::new(test_schemas());

        // Test filter with constant false -> Nothing
        let node = Node::Filter {
            source: Box::new(Node::Scan {
                table: "users".to_string(),
                alias: None,
            }),
            predicate: Expression::Constant(Value::Bool(false)),
        };

        let optimized = optimizer.optimize_node(node).unwrap();
        assert!(matches!(optimized, Node::Nothing));
    }

    #[test]
    fn test_constant_true_filter() {
        let optimizer = Optimizer::new(test_schemas());

        // Test filter with constant true -> removes filter
        let node = Node::Filter {
            source: Box::new(Node::Scan {
                table: "users".to_string(),
                alias: None,
            }),
            predicate: Expression::Constant(Value::Bool(true)),
        };

        let optimized = optimizer.optimize_node(node).unwrap();
        assert!(matches!(optimized, Node::Scan { .. }));
    }

    #[test]
    fn test_filter_combination() {
        let optimizer = Optimizer::new(test_schemas());

        // Test combining adjacent filters
        let node = Node::Filter {
            source: Box::new(Node::Filter {
                source: Box::new(Node::Scan {
                    table: "users".to_string(),
                    alias: None,
                }),
                predicate: Expression::GreaterThan(
                    Box::new(Expression::Column(2)),
                    Box::new(Expression::Constant(Value::integer(18))),
                ),
            }),
            predicate: Expression::LessThan(
                Box::new(Expression::Column(2)),
                Box::new(Expression::Constant(Value::integer(65))),
            ),
        };

        let optimized = optimizer.optimize_node(node).unwrap();

        // Should combine into single filter with AND
        if let Node::Filter { predicate, .. } = optimized {
            assert!(matches!(predicate, Expression::And(..)));
        } else {
            panic!("Expected combined filter");
        }
    }

    #[test]
    fn test_remove_redundant() {
        let optimizer = Optimizer::new(test_schemas());

        // Test LIMIT 0 -> Nothing
        let node = Node::Limit {
            source: Box::new(Node::Scan {
                table: "users".to_string(),
                alias: None,
            }),
            limit: 0,
        };

        let optimized = optimizer.optimize_node(node).unwrap();
        assert!(matches!(optimized, Node::Nothing));

        // Test OFFSET 0 removal
        let node = Node::Offset {
            source: Box::new(Node::Scan {
                table: "users".to_string(),
                alias: None,
            }),
            offset: 0,
        };

        let optimized = optimizer.optimize_node(node).unwrap();
        assert!(matches!(optimized, Node::Scan { .. }));
    }
}
