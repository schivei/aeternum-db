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

    #[test]
    fn test_transaction_commit_when_committed() {
        let mut tx = Transaction::new(1, IsolationLevel::ReadCommitted);
        assert!(tx.commit().is_ok());
        assert!(tx.commit().is_err());
    }

    #[test]
    fn test_transaction_commit_when_aborted() {
        let mut tx = Transaction::new(1, IsolationLevel::ReadCommitted);
        assert!(tx.rollback().is_ok());
        assert!(tx.commit().is_err());
    }

    #[test]
    fn test_transaction_rollback_when_committed() {
        let mut tx = Transaction::new(1, IsolationLevel::ReadCommitted);
        assert!(tx.commit().is_ok());
        assert!(tx.rollback().is_err());
    }

    #[test]
    fn test_transaction_rollback_when_aborted() {
        let mut tx = Transaction::new(1, IsolationLevel::ReadCommitted);
        assert!(tx.rollback().is_ok());
        assert!(tx.rollback().is_err());
    }

    #[test]
    fn test_isolation_levels() {
        let tx1 = Transaction::new(1, IsolationLevel::ReadUncommitted);
        let tx2 = Transaction::new(2, IsolationLevel::ReadCommitted);
        let tx3 = Transaction::new(3, IsolationLevel::RepeatableRead);
        let tx4 = Transaction::new(4, IsolationLevel::Serializable);

        assert_eq!(tx1.id(), 1);
        assert_eq!(tx2.id(), 2);
        assert_eq!(tx3.id(), 3);
        assert_eq!(tx4.id(), 4);
    }
}
