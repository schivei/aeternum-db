namespace AeternumDB.Core.Sql;

/// <summary>Errors produced by semantic validation.</summary>
public abstract class ValidationException : Exception
{
    protected ValidationException(string message) : base(message) { }

    public sealed class TableNotFoundException(string table)
        : ValidationException($"table '{table}' does not exist")
    {
        public string Table { get; } = table;
    }

    public sealed class ColumnNotFoundException(string table, string column)
        : ValidationException($"column '{column}' does not exist in table '{table}'")
    {
        public string Table { get; } = table;
        public string Column { get; } = column;
    }

    public sealed class TypeMismatchException(DataType expected, DataType found, string context)
        : ValidationException($"type mismatch in {context}: expected {expected}, found {found}")
    {
        public DataType Expected { get; } = expected;
        public DataType Found { get; } = found;
        public string Context { get; } = context;
    }

    public sealed class InvalidAggregateUsageException(string message)
        : ValidationException($"invalid aggregate usage: {message}") { }

    public sealed class NullConstraintViolationException(string table, string column)
        : ValidationException($"null constraint violation: column '{column}' in table '{table}' is NOT NULL")
    {
        public string Table { get; } = table;
        public string Column { get; } = column;
    }

    public sealed class ConstraintViolationException(string message)
        : ValidationException($"constraint violation: {message}") { }

    public sealed class TypeNotFoundException(string name)
        : ValidationException($"user-defined type '{name}' does not exist") { }

    public sealed class TypeInUseException(string name)
        : ValidationException($"cannot drop type '{name}': it is still referenced by one or more columns") { }

    public sealed class InvalidEnumValueException(string column, string value)
        : ValidationException($"invalid enum value '{value}' for column '{column}'")
    {
        public string Column { get; } = column;
        public string Value { get; } = value;
    }

    public sealed class NoActiveTransactionException()
        : ValidationException("no active transaction") { }

    public sealed class TransactionNameConflictException(string name)
        : ValidationException($"transaction name '{name}' is already in use in this session") { }

    public sealed class TransactionNotFoundException(string name)
        : ValidationException($"transaction '{name}' is not active in the current session") { }

    public sealed class TransactionNestingViolationException(string target, string blocking)
        : ValidationException($"cannot commit or rollback transaction '{target}': nested transaction '{blocking}' is still open")
    {
        public string Target { get; } = target;
        public string Blocking { get; } = blocking;
    }

    public sealed class ViewAsAggregateNotAllowedException(string func)
        : ValidationException($"aggregate function '{func}' is not allowed in a VIEW AS clause") { }

    public sealed class ViewAsSubqueryNotAllowedException()
        : ValidationException("sub-selects are not allowed in a VIEW AS clause") { }
}
