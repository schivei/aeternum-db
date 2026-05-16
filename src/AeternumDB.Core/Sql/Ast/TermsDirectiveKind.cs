namespace AeternumDB.Core.Sql.Ast;

/// <summary>Value kinds accepted by a TERMS directive in a full-text index definition.</summary>
public enum TermsDirectiveKind
{
    Text,
    Integer,
    Float,
    Boolean,
}
