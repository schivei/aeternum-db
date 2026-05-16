namespace AeternumDB.Core.Sql.Ast;

/// <summary>Actions triggered by foreign-key constraint violations.</summary>
public enum ReferentialAction
{
    Cascade,
    SetNull,
    SetDefault,
    Restrict,
    NoAction,
}
