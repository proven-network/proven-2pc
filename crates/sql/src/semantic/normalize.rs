//! Expression normalization for index matching
//!
//! Normalizes expressions to a canonical form so that semantically equivalent
//! expressions can be matched. For example, (a * 2) and (2 * a) should both
//! normalize to (a * 2).

use crate::types::expression::Expression;

/// Normalize an expression to a canonical form for matching
pub fn normalize_expression(expr: &Expression) -> Expression {
    match expr {
        // Commutative operations: put constants on the right
        Expression::Add(left, right) => {
            let left_norm = normalize_expression(left);
            let right_norm = normalize_expression(right);

            if is_constant(&left_norm) && !is_constant(&right_norm) {
                Expression::Add(Box::new(right_norm), Box::new(left_norm))
            } else {
                Expression::Add(Box::new(left_norm), Box::new(right_norm))
            }
        }

        Expression::Multiply(left, right) => {
            let left_norm = normalize_expression(left);
            let right_norm = normalize_expression(right);

            if is_constant(&left_norm) && !is_constant(&right_norm) {
                Expression::Multiply(Box::new(right_norm), Box::new(left_norm))
            } else {
                Expression::Multiply(Box::new(left_norm), Box::new(right_norm))
            }
        }

        Expression::And(left, right) => Expression::And(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::Or(left, right) => Expression::Or(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::Xor(left, right) => Expression::Xor(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        // Non-commutative operations: just normalize children
        Expression::Subtract(left, right) => Expression::Subtract(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::Divide(left, right) => Expression::Divide(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::Remainder(left, right) => Expression::Remainder(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::Exponentiate(left, right) => Expression::Exponentiate(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::Concat(left, right) => Expression::Concat(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        // Comparison operations
        Expression::Equal(left, right) => Expression::Equal(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::NotEqual(left, right) => Expression::NotEqual(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::GreaterThan(left, right) => Expression::GreaterThan(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::GreaterThanOrEqual(left, right) => Expression::GreaterThanOrEqual(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::LessThan(left, right) => Expression::LessThan(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::LessThanOrEqual(left, right) => Expression::LessThanOrEqual(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        // Bitwise operations
        Expression::BitwiseAnd(left, right) => Expression::BitwiseAnd(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::BitwiseOr(left, right) => Expression::BitwiseOr(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::BitwiseXor(left, right) => Expression::BitwiseXor(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::BitwiseShiftLeft(left, right) => Expression::BitwiseShiftLeft(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        Expression::BitwiseShiftRight(left, right) => Expression::BitwiseShiftRight(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
        ),

        // Unary operations
        Expression::Not(expr) => Expression::Not(Box::new(normalize_expression(expr))),
        Expression::Negate(expr) => Expression::Negate(Box::new(normalize_expression(expr))),
        Expression::Identity(expr) => Expression::Identity(Box::new(normalize_expression(expr))),
        Expression::Factorial(expr) => Expression::Factorial(Box::new(normalize_expression(expr))),
        Expression::BitwiseNot(expr) => {
            Expression::BitwiseNot(Box::new(normalize_expression(expr)))
        }

        // Pattern matching
        Expression::Like(left, right, negated) => Expression::Like(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
            *negated,
        ),

        Expression::ILike(left, right, negated) => Expression::ILike(
            Box::new(normalize_expression(left)),
            Box::new(normalize_expression(right)),
            *negated,
        ),

        Expression::Is(expr, val) => {
            Expression::Is(Box::new(normalize_expression(expr)), val.clone())
        }

        // Functions
        Expression::Function(name, args) => Expression::Function(
            name.clone(),
            args.iter().map(normalize_expression).collect(),
        ),

        // Collections
        Expression::InList(expr, list, negated) => Expression::InList(
            Box::new(normalize_expression(expr)),
            list.iter().map(normalize_expression).collect(),
            *negated,
        ),

        Expression::Between(expr, low, high, negated) => Expression::Between(
            Box::new(normalize_expression(expr)),
            Box::new(normalize_expression(low)),
            Box::new(normalize_expression(high)),
            *negated,
        ),

        Expression::ArrayLiteral(elements) => {
            Expression::ArrayLiteral(elements.iter().map(normalize_expression).collect())
        }

        Expression::MapLiteral(pairs) => Expression::MapLiteral(
            pairs
                .iter()
                .map(|(k, v)| (normalize_expression(k), normalize_expression(v)))
                .collect(),
        ),

        // Access operations
        Expression::ArrayAccess(base, index) => Expression::ArrayAccess(
            Box::new(normalize_expression(base)),
            Box::new(normalize_expression(index)),
        ),

        Expression::FieldAccess(base, field) => {
            Expression::FieldAccess(Box::new(normalize_expression(base)), field.clone())
        }

        // CASE expression
        Expression::Case {
            operand,
            when_clauses,
            else_clause,
        } => Expression::Case {
            operand: operand.as_ref().map(|o| Box::new(normalize_expression(o))),
            when_clauses: when_clauses
                .iter()
                .map(|(w, t)| (normalize_expression(w), normalize_expression(t)))
                .collect(),
            else_clause: else_clause
                .as_ref()
                .map(|e| Box::new(normalize_expression(e))),
        },

        // Cast: normalize the inner expression
        Expression::Cast(inner, target_type) => {
            Expression::Cast(Box::new(normalize_expression(inner)), target_type.clone())
        }

        // Subqueries and constants: return as-is
        Expression::InSubquery(_, _, _) => expr.clone(),
        Expression::Exists(_, _) => expr.clone(),
        Expression::Subquery(_) => expr.clone(),
        Expression::Constant(_) => expr.clone(),
        Expression::Column(_) => expr.clone(),
        Expression::Parameter(_) => expr.clone(),
        Expression::All => expr.clone(),
    }
}

/// Check if an expression is a constant value
fn is_constant(expr: &Expression) -> bool {
    matches!(expr, Expression::Constant(_))
}

/// Check if two normalized expressions are equal
pub fn expressions_equal(left: &Expression, right: &Expression) -> bool {
    match (left, right) {
        (Expression::Constant(v1), Expression::Constant(v2)) => v1 == v2,
        (Expression::Column(c1), Expression::Column(c2)) => c1 == c2,
        (Expression::Parameter(p1), Expression::Parameter(p2)) => p1 == p2,
        (Expression::All, Expression::All) => true,

        // Binary operations
        (Expression::Add(l1, r1), Expression::Add(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::Subtract(l1, r1), Expression::Subtract(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::Multiply(l1, r1), Expression::Multiply(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::Divide(l1, r1), Expression::Divide(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::Remainder(l1, r1), Expression::Remainder(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::Exponentiate(l1, r1), Expression::Exponentiate(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::Concat(l1, r1), Expression::Concat(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }

        // Logical operations
        (Expression::And(l1, r1), Expression::And(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::Or(l1, r1), Expression::Or(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::Xor(l1, r1), Expression::Xor(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }

        // Comparison operations
        (Expression::Equal(l1, r1), Expression::Equal(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::NotEqual(l1, r1), Expression::NotEqual(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::GreaterThan(l1, r1), Expression::GreaterThan(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::GreaterThanOrEqual(l1, r1), Expression::GreaterThanOrEqual(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::LessThan(l1, r1), Expression::LessThan(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::LessThanOrEqual(l1, r1), Expression::LessThanOrEqual(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }

        // Bitwise operations
        (Expression::BitwiseAnd(l1, r1), Expression::BitwiseAnd(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::BitwiseOr(l1, r1), Expression::BitwiseOr(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::BitwiseXor(l1, r1), Expression::BitwiseXor(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::BitwiseShiftLeft(l1, r1), Expression::BitwiseShiftLeft(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }
        (Expression::BitwiseShiftRight(l1, r1), Expression::BitwiseShiftRight(l2, r2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2)
        }

        // Unary operations
        (Expression::Not(e1), Expression::Not(e2)) => expressions_equal(e1, e2),
        (Expression::Negate(e1), Expression::Negate(e2)) => expressions_equal(e1, e2),
        (Expression::Identity(e1), Expression::Identity(e2)) => expressions_equal(e1, e2),
        (Expression::Factorial(e1), Expression::Factorial(e2)) => expressions_equal(e1, e2),
        (Expression::BitwiseNot(e1), Expression::BitwiseNot(e2)) => expressions_equal(e1, e2),

        // Pattern matching
        (Expression::Like(l1, r1, n1), Expression::Like(l2, r2, n2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2) && n1 == n2
        }
        (Expression::ILike(l1, r1, n1), Expression::ILike(l2, r2, n2)) => {
            expressions_equal(l1, l2) && expressions_equal(r1, r2) && n1 == n2
        }
        (Expression::Is(e1, v1), Expression::Is(e2, v2)) => expressions_equal(e1, e2) && v1 == v2,

        // Functions
        (Expression::Function(n1, args1), Expression::Function(n2, args2)) => {
            n1 == n2
                && args1.len() == args2.len()
                && args1
                    .iter()
                    .zip(args2.iter())
                    .all(|(a1, a2)| expressions_equal(a1, a2))
        }

        // Collections
        (Expression::InList(e1, l1, n1), Expression::InList(e2, l2, n2)) => {
            n1 == n2
                && expressions_equal(e1, e2)
                && l1.len() == l2.len()
                && l1
                    .iter()
                    .zip(l2.iter())
                    .all(|(i1, i2)| expressions_equal(i1, i2))
        }

        (Expression::Between(e1, low1, high1, n1), Expression::Between(e2, low2, high2, n2)) => {
            n1 == n2
                && expressions_equal(e1, e2)
                && expressions_equal(low1, low2)
                && expressions_equal(high1, high2)
        }

        (Expression::ArrayLiteral(elems1), Expression::ArrayLiteral(elems2)) => {
            elems1.len() == elems2.len()
                && elems1
                    .iter()
                    .zip(elems2.iter())
                    .all(|(e1, e2)| expressions_equal(e1, e2))
        }

        (Expression::MapLiteral(pairs1), Expression::MapLiteral(pairs2)) => {
            pairs1.len() == pairs2.len()
                && pairs1
                    .iter()
                    .zip(pairs2.iter())
                    .all(|((k1, v1), (k2, v2))| {
                        expressions_equal(k1, k2) && expressions_equal(v1, v2)
                    })
        }

        // Access operations
        (Expression::ArrayAccess(b1, i1), Expression::ArrayAccess(b2, i2)) => {
            expressions_equal(b1, b2) && expressions_equal(i1, i2)
        }

        (Expression::FieldAccess(b1, f1), Expression::FieldAccess(b2, f2)) => {
            expressions_equal(b1, b2) && f1 == f2
        }

        // Cast expressions
        (Expression::Cast(e1, t1), Expression::Cast(e2, t2)) => {
            t1 == t2 && expressions_equal(e1, e2)
        }

        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use proven_value::Value;

    use super::*;

    #[test]
    fn test_normalize_commutative_add() {
        let expr1 = Expression::Add(
            Box::new(Expression::Column(0)),
            Box::new(Expression::Constant(Value::I32(2))),
        );
        let expr2 = Expression::Add(
            Box::new(Expression::Constant(Value::I32(2))),
            Box::new(Expression::Column(0)),
        );

        let norm1 = normalize_expression(&expr1);
        let norm2 = normalize_expression(&expr2);

        assert!(expressions_equal(&norm1, &norm2));
    }

    #[test]
    fn test_normalize_commutative_multiply() {
        let expr1 = Expression::Multiply(
            Box::new(Expression::Column(0)),
            Box::new(Expression::Constant(Value::I32(2))),
        );
        let expr2 = Expression::Multiply(
            Box::new(Expression::Constant(Value::I32(2))),
            Box::new(Expression::Column(0)),
        );

        let norm1 = normalize_expression(&expr1);
        let norm2 = normalize_expression(&expr2);

        assert!(expressions_equal(&norm1, &norm2));
    }

    #[test]
    fn test_non_commutative_subtract() {
        let expr1 = Expression::Subtract(
            Box::new(Expression::Column(0)),
            Box::new(Expression::Constant(Value::I32(2))),
        );
        let expr2 = Expression::Subtract(
            Box::new(Expression::Constant(Value::I32(2))),
            Box::new(Expression::Column(0)),
        );

        let norm1 = normalize_expression(&expr1);
        let norm2 = normalize_expression(&expr2);

        // Should NOT be equal because subtraction is not commutative
        assert!(!expressions_equal(&norm1, &norm2));
    }
}
