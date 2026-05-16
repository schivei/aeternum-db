namespace AeternumDB.Core.Sql;

public abstract class CommitScope
{
    private CommitScope() { }

    public sealed class Current : CommitScope
    {
        public static readonly Current Instance = new();

        private Current() { }
    }

    public sealed class Named(string name) : CommitScope
    {
        public string Name { get; } = name;
    }

    public sealed class All : CommitScope
    {
        public static readonly All Instance = new();

        private All() { }
    }
}
