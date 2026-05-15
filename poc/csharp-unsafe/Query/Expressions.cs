using AeternumDB.PoC.Shared.Query;
using AeternumDB.PoC.Shared.Sql;
using AeternumDB.PoC.Shared.Types;
using AeternumDB.PoC.Unsafe.Executor;

namespace AeternumDB.PoC.Unsafe.Query;

/// <summary>
/// Simple expression evaluator for use in operator predicates and projections.
/// Mirrors Rust: executor/expressions.rs
/// </summary>
public static class ExprEvaluator
{
    public static DbValue Eval(Expr expr, DbRow row) => expr switch
    {
        Expr.Literal lit => LiteralToValue(lit.Value),
        Expr.Column col => row.Get(col.Name),
        Expr.BinaryOp bin => EvalBinary(bin, row),
        Expr.UnaryOp un => EvalUnary(un, row),
        Expr.IsNull isn => EvalIsNull(isn, row),
        Expr.Cast cast => EvalCast(cast, row),
        _ => DbValue.Null.Instance,
    };

    private static DbValue LiteralToValue(object? v) => v switch
    {
        null => DbValue.Null.Instance,
        bool b => new DbValue.Boolean(b),
        long l => new DbValue.Integer(l),
        int i => new DbValue.Integer(i),
        double d => new DbValue.Float(d),
        string s => new DbValue.Text(s),
        _ => new DbValue.Text(v.ToString() ?? ""),
    };

    private static DbValue EvalBinary(Expr.BinaryOp bin, DbRow row)
    {
        var l = Eval(bin.Left, row);
        var r = Eval(bin.Right, row);
        if (l.IsNull || r.IsNull) return DbValue.Null.Instance;
        return bin.Op.ToUpperInvariant() switch
        {
            "+" => Add(l, r),
            "-" => Sub(l, r),
            "*" => Mul(l, r),
            "/" => Div(l, r),
            "=" or "==" => new DbValue.Boolean(l.Equals(r)),
            "!=" or "<>" => new DbValue.Boolean(!l.Equals(r)),
            "<" => new DbValue.Boolean(Compare(l, r) < 0),
            "<=" => new DbValue.Boolean(Compare(l, r) <= 0),
            ">" => new DbValue.Boolean(Compare(l, r) > 0),
            ">=" => new DbValue.Boolean(Compare(l, r) >= 0),
            "AND" => new DbValue.Boolean(IsTrue(l) && IsTrue(r)),
            "OR" => new DbValue.Boolean(IsTrue(l) || IsTrue(r)),
            "LIKE" => EvalLike(l, r),
            _ => DbValue.Null.Instance,
        };
    }

    private static DbValue Add(DbValue l, DbValue r) => (l, r) switch
    {
        (DbValue.Integer a, DbValue.Integer b) => new DbValue.Integer(a.Value + b.Value),
        (DbValue.Float a, DbValue.Float b) => new DbValue.Float(a.Value + b.Value),
        (DbValue.Integer a, DbValue.Float b) => new DbValue.Float(a.Value + b.Value),
        (DbValue.Float a, DbValue.Integer b) => new DbValue.Float(a.Value + b.Value),
        (DbValue.Text a, DbValue.Text b) => new DbValue.Text(a.Value + b.Value),
        _ => DbValue.Null.Instance,
    };

    private static DbValue Sub(DbValue l, DbValue r) => (l, r) switch
    {
        (DbValue.Integer a, DbValue.Integer b) => new DbValue.Integer(a.Value - b.Value),
        (DbValue.Float a, DbValue.Float b) => new DbValue.Float(a.Value - b.Value),
        (DbValue.Integer a, DbValue.Float b) => new DbValue.Float(a.Value - b.Value),
        (DbValue.Float a, DbValue.Integer b) => new DbValue.Float(a.Value - b.Value),
        _ => DbValue.Null.Instance,
    };

    private static DbValue Mul(DbValue l, DbValue r) => (l, r) switch
    {
        (DbValue.Integer a, DbValue.Integer b) => new DbValue.Integer(a.Value * b.Value),
        (DbValue.Float a, DbValue.Float b) => new DbValue.Float(a.Value * b.Value),
        (DbValue.Integer a, DbValue.Float b) => new DbValue.Float(a.Value * b.Value),
        (DbValue.Float a, DbValue.Integer b) => new DbValue.Float(a.Value * b.Value),
        _ => DbValue.Null.Instance,
    };

    private static DbValue Div(DbValue l, DbValue r) => (l, r) switch
    {
        (DbValue.Integer _, DbValue.Integer b) when b.Value == 0 => DbValue.Null.Instance,
        (DbValue.Integer a, DbValue.Integer b) => new DbValue.Integer(a.Value / b.Value),
        (DbValue.Float a, DbValue.Float b) => new DbValue.Float(a.Value / b.Value),
        (DbValue.Integer a, DbValue.Float b) => new DbValue.Float(a.Value / b.Value),
        (DbValue.Float a, DbValue.Integer b) => new DbValue.Float(a.Value / b.Value),
        _ => DbValue.Null.Instance,
    };

    private static int Compare(DbValue l, DbValue r) => (l, r) switch
    {
        (DbValue.Integer a, DbValue.Integer b) => a.Value.CompareTo(b.Value),
        (DbValue.Float a, DbValue.Float b) => a.Value.CompareTo(b.Value),
        (DbValue.Integer a, DbValue.Float b) => ((double)a.Value).CompareTo(b.Value),
        (DbValue.Float a, DbValue.Integer b) => a.Value.CompareTo((double)b.Value),
        (DbValue.Text a, DbValue.Text b) => string.Compare(a.Value, b.Value, StringComparison.Ordinal),
        _ => 0,
    };

    private static DbValue EvalLike(DbValue l, DbValue r)
    {
        if (l is DbValue.Text lt && r is DbValue.Text rt)
        {
            var pattern = rt.Value.Replace("%", "*").Replace("_", "?");
            return new DbValue.Boolean(LikeMatch(lt.Value, pattern));
        }
        return new DbValue.Boolean(false);
    }

    private static bool LikeMatch(string input, string pattern)
    {
        // Simple glob-style match
        if (pattern == "*") return true;
        if (!pattern.Contains('*') && !pattern.Contains('?'))
            return input.Equals(pattern, StringComparison.OrdinalIgnoreCase);
        int i = 0, p = 0, starIdx = -1, match = 0;
        while (i < input.Length)
        {
            if (p < pattern.Length && (pattern[p] == '?' || pattern[p] == input[i]))
            { i++; p++; }
            else if (p < pattern.Length && pattern[p] == '*')
            { starIdx = p++; match = i; }
            else if (starIdx != -1)
            { p = starIdx + 1; i = ++match; }
            else return false;
        }
        while (p < pattern.Length && pattern[p] == '*') p++;
        return p == pattern.Length;
    }

    private static bool IsTrue(DbValue v) => v is DbValue.Boolean b && b.Value;

    private static DbValue EvalUnary(Expr.UnaryOp un, DbRow row)
    {
        var val = Eval(un.Operand, row);
        return un.Op.ToUpperInvariant() switch
        {
            "NOT" => val is DbValue.Boolean b ? new DbValue.Boolean(!b.Value) : DbValue.Null.Instance,
            "-" => val is DbValue.Integer i ? new DbValue.Integer(-i.Value)
                 : val is DbValue.Float f ? new DbValue.Float(-f.Value)
                 : DbValue.Null.Instance,
            _ => DbValue.Null.Instance,
        };
    }

    private static DbValue EvalIsNull(Expr.IsNull isn, DbRow row)
    {
        var val = Eval(isn.Inner, row);
        bool result = val.IsNull;
        return new DbValue.Boolean(isn.Negated ? !result : result);
    }

    private static DbValue EvalCast(Expr.Cast cast, DbRow row)
    {
        var val = Eval(cast.Inner, row);
        return cast.Type switch
        {
            DataType.Integer => val is DbValue.Integer ? val
                : val is DbValue.Float f ? new DbValue.Integer((long)f.Value)
                : val is DbValue.Text t && long.TryParse(t.Value, out var l) ? new DbValue.Integer(l)
                : DbValue.Null.Instance,
            DataType.Float => val is DbValue.Float ? val
                : val is DbValue.Integer i ? new DbValue.Float(i.Value)
                : val is DbValue.Text t && double.TryParse(t.Value, out var d) ? new DbValue.Float(d)
                : DbValue.Null.Instance,
            DataType.Text _ or DataType.Varchar _ => new DbValue.Text(val.ToString() ?? ""),
            DataType.Boolean => val is DbValue.Boolean ? val
                : val is DbValue.Integer i2 ? new DbValue.Boolean(i2.Value != 0)
                : DbValue.Null.Instance,
            _ => val,
        };
    }

    /// <summary>Build a <see cref="Func{DbRow,bool}"/> predicate from an Expr.</summary>
    public static Func<DbRow, bool> BuildPredicate(Expr expr)
    {
        return row =>
        {
            var v = Eval(expr, row);
            return v is DbValue.Boolean b && b.Value;
        };
    }
}

/// <summary>Built-in SQL aggregate accumulators. Mirrors Rust: executor/aggregate.rs</summary>
public static class Accumulators
{
    public static DbValue Count(IReadOnlyList<DbValue> vals) =>
        new DbValue.Integer(vals.Count(v => !v.IsNull));

    public static DbValue Sum(IReadOnlyList<DbValue> vals)
    {
        double s = 0;
        bool anyNonNull = false;
        foreach (var v in vals)
        {
            if (v is DbValue.Integer i) { s += i.Value; anyNonNull = true; }
            else if (v is DbValue.Float f) { s += f.Value; anyNonNull = true; }
        }
        return anyNonNull ? new DbValue.Float(s) : DbValue.Null.Instance;
    }

    public static DbValue Avg(IReadOnlyList<DbValue> vals)
    {
        var sum = Sum(vals);
        if (sum.IsNull) return DbValue.Null.Instance;
        long cnt = vals.Count(v => !v.IsNull);
        return cnt == 0 ? DbValue.Null.Instance
            : new DbValue.Float(((DbValue.Float)sum).Value / cnt);
    }

    public static DbValue Min(IReadOnlyList<DbValue> vals)
    {
        DbValue? min = null;
        foreach (var v in vals)
        {
            if (v.IsNull) continue;
            if (min is null || CompareDbValues(v, min) < 0) min = v;
        }
        return min ?? DbValue.Null.Instance;
    }

    public static DbValue Max(IReadOnlyList<DbValue> vals)
    {
        DbValue? max = null;
        foreach (var v in vals)
        {
            if (v.IsNull) continue;
            if (max is null || CompareDbValues(v, max) > 0) max = v;
        }
        return max ?? DbValue.Null.Instance;
    }

    private static int CompareDbValues(DbValue a, DbValue b) => (a, b) switch
    {
        (DbValue.Integer ia, DbValue.Integer ib) => ia.Value.CompareTo(ib.Value),
        (DbValue.Float fa, DbValue.Float fb) => fa.Value.CompareTo(fb.Value),
        (DbValue.Integer ia, DbValue.Float fb) => ((double)ia.Value).CompareTo(fb.Value),
        (DbValue.Float fa, DbValue.Integer ib) => fa.Value.CompareTo((double)ib.Value),
        (DbValue.Text ta, DbValue.Text tb) => string.Compare(ta.Value, tb.Value, StringComparison.Ordinal),
        _ => 0,
    };
}
