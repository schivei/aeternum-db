using AeternumDB.Core.Sql.Ast;

namespace AeternumDB.Core.Sql;

/// <summary>The kind of a user-defined type.</summary>
public abstract class UserTypeKind
{
    private UserTypeKind() { }

    public sealed class Enum(
        bool flag,
        IReadOnlyList<EnumVariant> variants,
        IReadOnlyList<ulong> resolvedValues
    ) : UserTypeKind
    {
        public bool Flag { get; } = flag;
        public IReadOnlyList<EnumVariant> Variants { get; } = variants;
        public IReadOnlyList<ulong> ResolvedValues { get; } = resolvedValues;
    }

    public sealed class Composite(IReadOnlyList<(string Name, DataType Type)> fields) : UserTypeKind
    {
        public IReadOnlyList<(string Name, DataType Type)> Fields { get; } = fields;
    }
}
