namespace AeternumDB.Core.Sql.Ast;

/// <summary>Discriminated union representing the storage index algorithm.</summary>
public abstract class IndexType
{
    private IndexType() { }

    #region Structural Index Types

    /// <summary>B-tree index: default balanced-tree index for ordered data.</summary>
    public sealed class BTree : IndexType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly BTree Instance = new();

        private BTree() { }
    }

    /// <summary>Hash index: equality-only index using a hash table.</summary>
    public sealed class Hash : IndexType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly Hash Instance = new();

        private Hash() { }
    }

    #endregion

    #region Generalized Index Types

    /// <summary>GIN (Generalized Inverted Index): suitable for composite values such as arrays and full-text.</summary>
    public sealed class Gin : IndexType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly Gin Instance = new();

        private Gin() { }
    }

    /// <summary>GiST (Generalized Search Tree): suitable for geometric and range data.</summary>
    public sealed class Gist : IndexType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly Gist Instance = new();

        private Gist() { }
    }

    /// <summary>SP-GiST (Space-Partitioned GiST): suitable for non-balanced, partitioned structures.</summary>
    public sealed class SpGist : IndexType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly SpGist Instance = new();

        private SpGist() { }
    }

    #endregion

    #region Specialized Index Types

    /// <summary>BRIN (Block Range Index): compact index for large, naturally ordered tables.</summary>
    public sealed class Brin : IndexType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly Brin Instance = new();

        private Brin() { }
    }

    /// <summary>Bloom filter index: probabilistic index for multi-column equality queries.</summary>
    public sealed class Bloom : IndexType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly Bloom Instance = new();

        private Bloom() { }
    }

    /// <summary>Full-text search index.</summary>
    public sealed class FullText : IndexType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly FullText Instance = new();

        private FullText() { }
    }

    /// <summary>Trigram index: supports substring and similarity searches.</summary>
    public sealed class Trigram : IndexType
    {
        /// <summary>The singleton instance.</summary>
        public static readonly Trigram Instance = new();

        private Trigram() { }
    }

    #endregion

    #region Extension Index Types

    /// <summary>A named index type not covered by the built-in variants.</summary>
    public sealed class Other(string name) : IndexType
    {
        /// <summary>The custom index type name.</summary>
        public string Name { get; } = name;
    }

    #endregion
}
