// Decimal Engine for numeric precision
// Licensed under AGPLv3.0

use rust_decimal::Decimal;

/// Decimal arithmetic operations with precision
pub struct DecimalEngine;

impl DecimalEngine {
    /// Add two decimal numbers with precision
    pub fn add(a: Decimal, b: Decimal) -> Decimal {
        a + b
    }

    /// Subtract two decimal numbers with precision
    pub fn subtract(a: Decimal, b: Decimal) -> Decimal {
        a - b
    }

    /// Multiply two decimal numbers with precision
    pub fn multiply(a: Decimal, b: Decimal) -> Decimal {
        a * b
    }

    /// Divide two decimal numbers with precision
    pub fn divide(a: Decimal, b: Decimal) -> Result<Decimal, String> {
        if b.is_zero() {
            return Err("Division by zero".to_string());
        }
        Ok(a / b)
    }

    /// Parse string to decimal
    pub fn from_str(s: &str) -> Result<Decimal, String> {
        s.parse::<Decimal>()
            .map_err(|e| format!("Failed to parse decimal: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal_addition() {
        let a = Decimal::new(100, 2); // 1.00
        let b = Decimal::new(50, 2); // 0.50
        let result = DecimalEngine::add(a, b);
        assert_eq!(result, Decimal::new(150, 2)); // 1.50
    }

    #[test]
    fn test_decimal_division() {
        let a = Decimal::new(100, 2); // 1.00
        let b = Decimal::new(50, 2); // 0.50
        let result = DecimalEngine::divide(a, b).unwrap();
        assert_eq!(result, Decimal::new(200, 2)); // 2.00
    }

    #[test]
    fn test_division_by_zero() {
        let a = Decimal::new(100, 2);
        let b = Decimal::ZERO;
        assert!(DecimalEngine::divide(a, b).is_err());
    }
}
