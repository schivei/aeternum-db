namespace AeternumDB.Core.Errors;

// ── Storage ──────────────────────────────────────────────────────────────────

public enum StorageErrorKind
{
    BufferPool,
    FileManager,
    OutOfBounds,
    ChecksumMismatch,
    PagePinned,
}

public sealed class StorageException(StorageErrorKind kind, string message)
    : Exception(message)
{
    public StorageErrorKind Kind { get; } = kind;
}

// ── Index ─────────────────────────────────────────────────────────────────────

public enum IndexErrorKind
{
    Storage,
    Serialization,
    KeyNotFound,
    InvalidFanout,
    TreeCorrupted,
}

public sealed class IndexException(IndexErrorKind kind, string message)
    : Exception(message)
{
    public IndexErrorKind Kind { get; } = kind;
}

// ── Executor ──────────────────────────────────────────────────────────────────

public enum ExecutorErrorKind
{
    TableNotFound,
    ColumnNotFound,
    TypeMismatch,
    EvalError,
    IoError,
    PermissionDenied,
    ReferentialIntegrityViolation,
    Other,
}

public sealed class ExecutorException(ExecutorErrorKind kind, string message)
    : Exception(message)
{
    public ExecutorErrorKind Kind { get; } = kind;
}

// ── Query Planner ─────────────────────────────────────────────────────────────

public enum PlannerErrorKind
{
    UnsupportedStatement,
    CatalogError,
    CrossDatabaseJoin,
    FlatTableJoin,
    InvalidExpand,
    Other,
}

public sealed class PlannerException(PlannerErrorKind kind, string message)
    : Exception(message)
{
    public PlannerErrorKind Kind { get; } = kind;
}
