namespace AeternumDB.Core.Sql;

/// <summary>Classifies parser-related SQL errors.</summary>
public enum SqlErrorKind
{
    /// <summary>Represents a generic SQL parsing failure.</summary>
    ParseError,
    /// <summary>Represents a failure while constructing or validating AST shape.</summary>
    AstError,
    /// <summary>Represents an empty SQL input payload.</summary>
    EmptyInput,
}
