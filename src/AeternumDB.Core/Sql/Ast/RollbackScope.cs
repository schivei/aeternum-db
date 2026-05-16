namespace AeternumDB.Core.Sql.Ast;

public abstract class RollbackScope
{
    private RollbackScope() { }

    public sealed class Current : RollbackScope
    {
        public static readonly Current Instance = new();

        private Current() { }
    }

    public sealed class ToSavepoint(string name) : RollbackScope
    {
        public string Name { get; } = name;
    }

    public sealed class Named(string name) : RollbackScope
    {
        public string Name { get; } = name;
    }

    public sealed class All : RollbackScope
    {
        public static readonly All Instance = new();

        private All() { }
    }
}
