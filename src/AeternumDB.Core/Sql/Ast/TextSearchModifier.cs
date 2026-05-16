namespace AeternumDB.Core.Sql.Ast;

/// <summary>Modifiers for full-text MATCH … AGAINST expressions.</summary>
public enum TextSearchModifier
{
    NaturalLanguage,
    NaturalLanguageWithExpansion,
    Boolean,
    WithExpansion,
}
