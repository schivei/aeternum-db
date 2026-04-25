// ACID Transaction Engine
// Licensed under AGPLv3.0

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

/// Represents an ACID transaction
pub struct Transaction {
    id: u64,
    isolation_level: IsolationLevel,
    state: TransactionState,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(id: u64, isolation_level: IsolationLevel) -> Self {
        Transaction {
            id,
            isolation_level,
            state: TransactionState::Active,
        }
    }

    /// Get transaction ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get current state
    pub fn state(&self) -> TransactionState {
        self.state
    }

    /// Commit transaction
    pub fn commit(&mut self) -> Result<(), String> {
        if self.state != TransactionState::Active {
            return Err("Transaction is not active".to_string());
        }
        self.state = TransactionState::Committed;
        Ok(())
    }

    /// Rollback transaction
    pub fn rollback(&mut self) -> Result<(), String> {
        if self.state != TransactionState::Active {
            return Err("Transaction is not active".to_string());
        }
        self.state = TransactionState::Aborted;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_creation() {
        let tx = Transaction::new(1, IsolationLevel::ReadCommitted);
        assert_eq!(tx.id(), 1);
        assert_eq!(tx.state(), TransactionState::Active);
    }

    #[test]
    fn test_transaction_commit() {
        let mut tx = Transaction::new(1, IsolationLevel::ReadCommitted);
        assert!(tx.commit().is_ok());
        assert_eq!(tx.state(), TransactionState::Committed);
    }

    #[test]
    fn test_transaction_rollback() {
        let mut tx = Transaction::new(1, IsolationLevel::ReadCommitted);
        assert!(tx.rollback().is_ok());
        assert_eq!(tx.state(), TransactionState::Aborted);
    }
}
