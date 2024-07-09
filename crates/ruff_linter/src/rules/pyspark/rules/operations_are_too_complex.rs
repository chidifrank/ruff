use crate::checkers::ast::Checker;
use ruff_diagnostics::{Diagnostic, Violation};
use ruff_macros::{derive_message_formats, violation};
use ruff_python_ast::{self as ast, Expr, ExprCall};
use ruff_text_size::Ranged;

/// ## What it does
/// Checks for pyspark operations using .when() that are too complex.
///
/// ## Why is this bad?
/// Logical operations, which often reside inside F.when(), need to be readable.
/// We apply the same rule as with chaining functions, keeping logic expressions inside the
/// same code block to three (3) expressions at most. If they grow longer, it is often a sign that
/// the code can be simplified or extracted out. Extracting out complex logical operations into
/// variables makes the code easier to read and reason about, which also reduces bugs.
///
/// Functions with a high complexity are hard to understand and maintain.
///
/// ## Example
/// ```python
/// import pyspark.sql.functions as F
///
/// F.when((F.col('prod_status') == 'Delivered') | (((F.datediff('deliveryDate_actual', 'current_date') < 0)
/// & ((F.col('currentRegistration') != '') | ((F.datediff('deliveryDate_actual', 'current_date') < 0)
/// & ((F.col('originalOperator') != '') | (F.col('currentOperator') != '')))))), 'In Service')
/// ```
///
/// Use instead:
/// ```python
/// import pyspark.sql.functions as F
///
/// has_operator = ((F.col('originalOperator') != '') | (F.col('currentOperator') != ''))
/// delivery_date_passed = (F.datediff('deliveryDate_actual', 'current_date') < 0)
/// has_registration = (F.col('currentRegistration').rlike('.+'))
/// is_delivered = (F.col('prod_status') == 'Delivered')
/// is_active = (has_registration | has_operator)
///
/// F.when(is_delivered | (delivery_date_passed & is_active), 'In Service')
/// ```
/// ## Options
/// - `pyspark.max-complexity`

#[violation]
pub struct SparkComplexStructure {
    complexity: usize,
    max_complexity: usize,
}

impl Violation for SparkComplexStructure {
    #[derive_message_formats]
    fn message(&self) -> String {
        let SparkComplexStructure {
            complexity,
            max_complexity,
        } = self;
        format!("when() is too complex ({complexity} > {max_complexity})")
    }
}

fn get_binary_op_expr_complexity(expr: &Expr, checker: &Checker) -> usize {
    match expr {
        Expr::BinOp(ast::ExprBinOp { left, right, .. }) => {
            // Calculate the complexity recursively for both sides of the binary operation
            let left_complexity = get_binary_op_expr_complexity(left, checker);
            let right_complexity = get_binary_op_expr_complexity(right, checker);

            // Total complexity is the sum of complexities of both sides plus 1 for the current operation
            1 + left_complexity + right_complexity
        }
        Expr::Call(ExprCall {
            func, arguments, ..
        }) if is_pyspark_function_when_call(func, checker) => {
            // Start with the complexity of the function call itself
            let mut complexity = get_binary_op_expr_complexity(func, checker);

            // Calculate the complexity recursively for all arguments of the function call
            complexity += arguments
                .args
                .iter()
                .map(|arg| get_binary_op_expr_complexity(arg, checker))
                .sum::<usize>();

            // Return the total complexity
            complexity
        }
        _ => 0,
    }
}

fn is_pyspark_function_when_call(func: &Expr, checker: &Checker) -> bool {
    checker
        .semantic()
        .resolve_qualified_name(func)
        .is_some_and(|qualified_name| {
            matches!(
                qualified_name.segments(),
                ["pyspark", "sql", "functions", "when"]
            )
        })
}

fn get_call_complexity(call: &ExprCall, checker: &Checker) -> usize {
    get_binary_op_expr_complexity(&Expr::Call(call.clone()), checker)
}

/// PS001
pub(crate) fn operations_are_too_complex(
    checker: &mut Checker,
    call: &ExprCall,
    max_complexity: usize,
) {
    if is_pyspark_function_when_call(&call.func, checker) {
        let complexity = get_call_complexity(call, checker);

        if complexity > max_complexity {
            checker.diagnostics.push(Diagnostic::new(
                SparkComplexStructure {
                    complexity,
                    max_complexity,
                },
                call.func.range(),
            ));
        }
    }
}
