// SQL expression evaluator with NULL propagation and core SQL functions.
// Transpiled from poc/rust/src/executor/expressions.rs.

namespace AeternumDB.Core.Executor;

using System.Text;
using System.Text.RegularExpressions;
using AeternumDB.Core.Errors;
using AeternumDB.Core.Sql;
using AeternumDB.Core.Types;

/// <summary>
/// Evaluates SQL expressions against a row, propagating NULL as per SQL semantics.
/// </summary>
public static class ExpressionEvaluator
{
    /// <summary>Evaluate an expression against a row, returning a DbValue.</summary>
    public static DbValue Eval(Expr expr, DbRow row) =>
        expr switch
        {
            Expr.Literal lit => EvalLiteral(lit.Value),
            Expr.Column col => EvalColumn(col.Table, col.Name, row),
            Expr.BinaryOp bin => EvalBinaryOp(bin.Left, bin.Op, bin.Right, row),
            Expr.UnaryOp un => EvalUnaryOp(un.Op, un.Inner, row),
            Expr.Function fn => EvalFunction(fn.Name, fn.Args, row),
            Expr.Case c => EvalCase(c.Operand, c.Conditions, c.ElseResult, row),
            Expr.Cast cast => EvalCast(cast.Inner, cast.DataType, row),
            Expr.Between bet => EvalBetween(bet.Inner, bet.Low, bet.High, bet.Negated, row),
            Expr.InList inList => EvalInList(inList.Inner, inList.List, inList.Negated, row),
            Expr.IsNull isNull => EvalIsNull(isNull.Inner, isNull.Negated, row),
            Expr.Wildcard => DbValue.Null.Instance,
            Expr.Substring sub => EvalSubstring(sub.Inner, sub.FromPos, sub.Len, row),
            Expr.Trim trim => EvalTrim(trim.Inner, trim.TrimWhere, trim.TrimWhat, row),
            Expr.Position pos => EvalPosition(pos.Substr, pos.InExpr, row),
            _ => throw new ExecutorException(ExecutorErrorKind.EvalError,
                $"Unsupported expression: {expr.GetType().Name}")
        };

    // ── Literal ───────────────────────────────────────────────────────────────

    private static DbValue EvalLiteral(SqlValue val) =>
        val switch
        {
            SqlValue.Null => DbValue.Null.Instance,
            SqlValue.Boolean b => new DbValue.Boolean(b.Value),
            SqlValue.Integer i => new DbValue.Integer(i.Value),
            SqlValue.Float f => new DbValue.Float(f.Value),
            SqlValue.SqlString s => new DbValue.Text(s.Value),
            _ => throw new ExecutorException(ExecutorErrorKind.EvalError,
                $"Unknown literal type: {val.GetType().Name}")
        };

    // ── Column lookup ─────────────────────────────────────────────────────────

    private static DbValue EvalColumn(string? table, string name, DbRow row)
    {
        if (table is not null)
        {
            var qualified = $"{table}.{name}";
            var qval = row.Get(qualified);
            if (!qval.IsNull) return qval;

            // Also try unqualified in case data was stored without prefix
            var uval = row.Get(name);
            if (!uval.IsNull) return uval;

            // Return null only if we truly can't find it — column may have a null value stored
            // Distinguish "stored null" from "column missing" by checking column set.
            foreach (var kv in row.Columns)
            {
                if (string.Equals(kv.Key, qualified, StringComparison.OrdinalIgnoreCase)
                    || string.Equals(kv.Key, name, StringComparison.OrdinalIgnoreCase))
                    return kv.Value;
            }
            throw new ExecutorException(ExecutorErrorKind.ColumnNotFound, $"Column not found: {qualified}");
        }
        else
        {
            // Check if the column exists (could be stored as Null)
            foreach (var kv in row.Columns)
            {
                if (string.Equals(kv.Key, name, StringComparison.OrdinalIgnoreCase))
                    return kv.Value;
            }
            // Column not in row; for unqualified look-up this may just be NULL
            return DbValue.Null.Instance;
        }
    }

    // ── Binary operators ──────────────────────────────────────────────────────

    private static DbValue EvalBinaryOp(Expr left, BinaryOperator op, Expr right, DbRow row)
    {
        // AND/OR use 3-valued logic with short-circuit
        if (op == BinaryOperator.And) return EvalAnd(left, right, row);
        if (op == BinaryOperator.Or) return EvalOr(left, right, row);

        var lv = Eval(left, row);
        var rv = Eval(right, row);

        if (lv.IsNull || rv.IsNull)
            return DbValue.Null.Instance;

        return op switch
        {
            BinaryOperator.Plus => EvalAdd(lv, rv),
            BinaryOperator.Minus => EvalSub(lv, rv),
            BinaryOperator.Multiply => EvalMul(lv, rv),
            BinaryOperator.Divide => EvalDiv(lv, rv),
            BinaryOperator.Modulo => EvalMod(lv, rv),
            BinaryOperator.Eq => new DbValue.Boolean(CompareDbValues(lv, rv) == 0),
            BinaryOperator.NotEq => new DbValue.Boolean(CompareDbValues(lv, rv) != 0),
            BinaryOperator.Lt => new DbValue.Boolean(CompareDbValues(lv, rv) < 0),
            BinaryOperator.LtEq => new DbValue.Boolean(CompareDbValues(lv, rv) <= 0),
            BinaryOperator.Gt => new DbValue.Boolean(CompareDbValues(lv, rv) > 0),
            BinaryOperator.GtEq => new DbValue.Boolean(CompareDbValues(lv, rv) >= 0),
            BinaryOperator.StringConcat => EvalConcat(lv, rv),
            BinaryOperator.Like => EvalLikeOp(lv, rv, caseInsensitive: false, negated: false),
            BinaryOperator.NotLike => EvalLikeOp(lv, rv, caseInsensitive: false, negated: true),
            BinaryOperator.ILike => EvalLikeOp(lv, rv, caseInsensitive: true, negated: false),
            BinaryOperator.NotILike => EvalLikeOp(lv, rv, caseInsensitive: true, negated: true),
            _ => throw new ExecutorException(ExecutorErrorKind.EvalError,
                $"Unsupported binary operator: {op}")
        };
    }

    private static DbValue EvalAnd(Expr left, Expr right, DbRow row)
    {
        var lv = Eval(left, row);
        if (lv.AsBool() == false) return new DbValue.Boolean(false);
        var rv = Eval(right, row);
        return (lv.AsBool(), rv.AsBool()) switch
        {
            (true, true) => new DbValue.Boolean(true),
            (false, _) or (_, false) => new DbValue.Boolean(false),
            _ => DbValue.Null.Instance
        };
    }

    private static DbValue EvalOr(Expr left, Expr right, DbRow row)
    {
        var lv = Eval(left, row);
        if (lv.AsBool() == true) return new DbValue.Boolean(true);
        var rv = Eval(right, row);
        return (lv.AsBool(), rv.AsBool()) switch
        {
            (true, _) or (_, true) => new DbValue.Boolean(true),
            (false, false) => new DbValue.Boolean(false),
            _ => DbValue.Null.Instance
        };
    }

    // ── Arithmetic ────────────────────────────────────────────────────────────

    private static DbValue EvalAdd(DbValue lv, DbValue rv) =>
        (lv, rv) switch
        {
            (DbValue.Integer a, DbValue.Integer b) => new DbValue.Integer(a.Value + b.Value),
            (DbValue.Float a, DbValue.Float b) => new DbValue.Float(a.Value + b.Value),
            (DbValue.Integer a, DbValue.Float b) => new DbValue.Float(a.Value + b.Value),
            (DbValue.Float a, DbValue.Integer b) => new DbValue.Float(a.Value + b.Value),
            _ => throw TypeMismatch("numeric", lv, rv)
        };

    private static DbValue EvalSub(DbValue lv, DbValue rv) =>
        (lv, rv) switch
        {
            (DbValue.Integer a, DbValue.Integer b) => new DbValue.Integer(a.Value - b.Value),
            (DbValue.Float a, DbValue.Float b) => new DbValue.Float(a.Value - b.Value),
            (DbValue.Integer a, DbValue.Float b) => new DbValue.Float(a.Value - b.Value),
            (DbValue.Float a, DbValue.Integer b) => new DbValue.Float(a.Value - b.Value),
            _ => throw TypeMismatch("numeric", lv, rv)
        };

    private static DbValue EvalMul(DbValue lv, DbValue rv) =>
        (lv, rv) switch
        {
            (DbValue.Integer a, DbValue.Integer b) => new DbValue.Integer(a.Value * b.Value),
            (DbValue.Float a, DbValue.Float b) => new DbValue.Float(a.Value * b.Value),
            (DbValue.Integer a, DbValue.Float b) => new DbValue.Float(a.Value * b.Value),
            (DbValue.Float a, DbValue.Integer b) => new DbValue.Float(a.Value * b.Value),
            _ => throw TypeMismatch("numeric", lv, rv)
        };

    private static DbValue EvalDiv(DbValue lv, DbValue rv)
    {
        return (lv, rv) switch
        {
            (DbValue.Integer a, DbValue.Integer b) when b.Value == 0
                => throw new ExecutorException(ExecutorErrorKind.EvalError, "Division by zero"),
            (DbValue.Integer a, DbValue.Integer b) => new DbValue.Integer(a.Value / b.Value),
            (DbValue.Float a, DbValue.Float b) when b.Value == 0.0
                => throw new ExecutorException(ExecutorErrorKind.EvalError, "Division by zero"),
            (DbValue.Float a, DbValue.Float b) => new DbValue.Float(a.Value / b.Value),
            (DbValue.Integer a, DbValue.Float b) when b.Value == 0.0
                => throw new ExecutorException(ExecutorErrorKind.EvalError, "Division by zero"),
            (DbValue.Integer a, DbValue.Float b) => new DbValue.Float(a.Value / b.Value),
            (DbValue.Float a, DbValue.Integer b) when b.Value == 0
                => throw new ExecutorException(ExecutorErrorKind.EvalError, "Division by zero"),
            (DbValue.Float a, DbValue.Integer b) => new DbValue.Float(a.Value / b.Value),
            _ => throw TypeMismatch("numeric", lv, rv)
        };
    }

    private static DbValue EvalMod(DbValue lv, DbValue rv) =>
        (lv, rv) switch
        {
            (DbValue.Integer a, DbValue.Integer b) when b.Value == 0
                => throw new ExecutorException(ExecutorErrorKind.EvalError, "Modulo by zero"),
            (DbValue.Integer a, DbValue.Integer b) => new DbValue.Integer(a.Value % b.Value),
            _ => throw TypeMismatch("integer", lv, rv)
        };

    private static DbValue EvalConcat(DbValue lv, DbValue rv) =>
        new DbValue.Text((lv.AsString() ?? lv.ToString()!) + (rv.AsString() ?? rv.ToString()!));

    // ── LIKE / ILIKE ──────────────────────────────────────────────────────────

    private static DbValue EvalLikeOp(DbValue lv, DbValue rv, bool caseInsensitive, bool negated)
    {
        if (lv is not DbValue.Text lText || rv is not DbValue.Text rText)
            throw new ExecutorException(ExecutorErrorKind.TypeMismatch,
                "LIKE operator requires string operands");

        string pattern = LikeToRegex(rText.Value);
        var opts = caseInsensitive ? RegexOptions.IgnoreCase : RegexOptions.None;
        bool matches;
        try
        {
            matches = Regex.IsMatch(lText.Value, pattern, opts, TimeSpan.FromSeconds(1));
        }
        catch (RegexParseException)
        {
            throw new ExecutorException(ExecutorErrorKind.EvalError,
                $"Invalid LIKE pattern: {rText.Value}");
        }
        catch (RegexMatchTimeoutException)
        {
            throw new ExecutorException(ExecutorErrorKind.EvalError,
                "LIKE pattern evaluation timed out");
        }
        return new DbValue.Boolean(negated ? !matches : matches);
    }

    private static string LikeToRegex(string pattern)
    {
        var sb = new StringBuilder("^");
        for (int i = 0; i < pattern.Length; i++)
        {
            char c = pattern[i];
            if (c == '%') sb.Append(".*");
            else if (c == '_') sb.Append('.');
            else if (c == '\\' && i + 1 < pattern.Length)
                sb.Append(Regex.Escape(pattern[++i].ToString()));
            else
                sb.Append(Regex.Escape(c.ToString()));
        }
        sb.Append('$');
        return sb.ToString();
    }

    // ── Unary operators ───────────────────────────────────────────────────────

    private static DbValue EvalUnaryOp(UnaryOperator op, Expr inner, DbRow row)
    {
        var val = Eval(inner, row);
        if (val.IsNull) return DbValue.Null.Instance;

        return op switch
        {
            UnaryOperator.Not => val.AsBool() is bool b
                ? new DbValue.Boolean(!b)
                : throw new ExecutorException(ExecutorErrorKind.TypeMismatch,
                    $"NOT requires boolean, got {val}"),
            UnaryOperator.Minus => val switch
            {
                DbValue.Integer i => new DbValue.Integer(-i.Value),
                DbValue.Float f => new DbValue.Float(-f.Value),
                _ => throw TypeMismatch("numeric", val)
            },
            _ => val
        };
    }

    // ── Functions ─────────────────────────────────────────────────────────────

    private static DbValue EvalFunction(string name, IReadOnlyList<Expr> argExprs, DbRow row)
    {
        var args = new DbValue[argExprs.Count];
        for (int i = 0; i < argExprs.Count; i++)
            args[i] = Eval(argExprs[i], row);

        return name.ToUpperInvariant() switch
        {
            "COALESCE" => EvalCoalesce(args),
            "ABS" => EvalAbs(args),
            "LOWER" => EvalLower(args),
            "UPPER" => EvalUpper(args),
            "LENGTH" or "LEN" or "CHAR_LENGTH" or "CHARACTER_LENGTH" => EvalLength(args),
            "TRIM" => EvalTrimFn(args),
            "LTRIM" => EvalLtrim(args),
            "RTRIM" => EvalRtrim(args),
            "ROUND" => EvalRound(args),
            "FLOOR" => EvalFloor(args),
            "CEIL" or "CEILING" => EvalCeil(args),
            "SQRT" => EvalSqrt(args),
            "SUBSTR" or "SUBSTRING" => EvalSubstringFn(args),
            "CONCAT" => EvalConcatFn(args),
            "REPLACE" => EvalReplace(args),
            "NULLIF" => EvalNullIf(args),
            "NVL" or "IFNULL" => EvalCoalesce(args),
            "COUNT" => new DbValue.Integer(args.Count(a => !a.IsNull)),
            "MOD" => args.Length == 2 ? EvalMod(args[0], args[1]) : throw ArgCountError("MOD", 2, args.Length),
            "POWER" or "POW" => EvalPow(args),
            "EXP" => EvalExp(args),
            "LN" or "LOG" => EvalLog(args),
            "SIGN" => EvalSign(args),
            _ => throw new ExecutorException(ExecutorErrorKind.EvalError, $"Unknown function: {name}")
        };
    }

    private static DbValue EvalCoalesce(DbValue[] args)
    {
        foreach (var v in args)
            if (!v.IsNull) return v;
        return DbValue.Null.Instance;
    }

    private static DbValue EvalAbs(DbValue[] args)
    {
        RequireOne("ABS", args, out var val);
        return val switch
        {
            null => DbValue.Null.Instance,
            DbValue.Integer i => new DbValue.Integer(Math.Abs(i.Value)),
            DbValue.Float f => new DbValue.Float(Math.Abs(f.Value)),
            _ => throw TypeMismatch("numeric", args[0])
        };
    }

    private static DbValue EvalLower(DbValue[] args)
    {
        RequireOneString("LOWER", args, out var s);
        return s is null ? DbValue.Null.Instance : new DbValue.Text(s.ToLowerInvariant());
    }

    private static DbValue EvalUpper(DbValue[] args)
    {
        RequireOneString("UPPER", args, out var s);
        return s is null ? DbValue.Null.Instance : new DbValue.Text(s.ToUpperInvariant());
    }

    private static DbValue EvalLength(DbValue[] args)
    {
        RequireOneString("LENGTH", args, out var s);
        return s is null ? DbValue.Null.Instance : new DbValue.Integer(s.Length);
    }

    private static DbValue EvalTrimFn(DbValue[] args)
    {
        RequireOneString("TRIM", args, out var s);
        return s is null ? DbValue.Null.Instance : new DbValue.Text(s.Trim());
    }

    private static DbValue EvalLtrim(DbValue[] args)
    {
        RequireOneString("LTRIM", args, out var s);
        return s is null ? DbValue.Null.Instance : new DbValue.Text(s.TrimStart());
    }

    private static DbValue EvalRtrim(DbValue[] args)
    {
        RequireOneString("RTRIM", args, out var s);
        return s is null ? DbValue.Null.Instance : new DbValue.Text(s.TrimEnd());
    }

    private static DbValue EvalRound(DbValue[] args)
    {
        if (args.Length == 0 || args.Length > 2)
            throw new ExecutorException(ExecutorErrorKind.EvalError, "ROUND requires 1-2 arguments");
        if (args[0].IsNull) return DbValue.Null.Instance;
        int decimals = args.Length > 1 ? (int)(args[1].AsInteger() ?? 0) : 0;
        return args[0] switch
        {
            DbValue.Float f => new DbValue.Float(Math.Round(f.Value, decimals, MidpointRounding.AwayFromZero)),
            DbValue.Integer i => new DbValue.Integer(i.Value),
            _ => throw TypeMismatch("numeric", args[0])
        };
    }

    private static DbValue EvalFloor(DbValue[] args)
    {
        RequireOne("FLOOR", args, out var val);
        return val switch
        {
            null => DbValue.Null.Instance,
            DbValue.Float f => new DbValue.Integer((long)Math.Floor(f.Value)),
            DbValue.Integer i => new DbValue.Integer(i.Value),
            _ => throw TypeMismatch("numeric", args[0])
        };
    }

    private static DbValue EvalCeil(DbValue[] args)
    {
        RequireOne("CEIL", args, out var val);
        return val switch
        {
            null => DbValue.Null.Instance,
            DbValue.Float f => new DbValue.Integer((long)Math.Ceiling(f.Value)),
            DbValue.Integer i => new DbValue.Integer(i.Value),
            _ => throw TypeMismatch("numeric", args[0])
        };
    }

    private static DbValue EvalSqrt(DbValue[] args)
    {
        RequireOne("SQRT", args, out var val);
        return val switch
        {
            null => DbValue.Null.Instance,
            DbValue.Float f => new DbValue.Float(Math.Sqrt(f.Value)),
            DbValue.Integer i => new DbValue.Float(Math.Sqrt(i.Value)),
            _ => throw TypeMismatch("numeric", args[0])
        };
    }

    private static DbValue EvalSubstringFn(DbValue[] args)
    {
        if (args.Length < 2 || args[0].IsNull) return DbValue.Null.Instance;
        if (args[0] is not DbValue.Text t)
            throw TypeMismatch("string", args[0]);
        int from = (int)(args[1].AsInteger() ?? 1) - 1;
        if (from < 0) from = 0;
        if (from >= t.Value.Length) return new DbValue.Text(string.Empty);
        if (args.Length >= 3 && !args[2].IsNull)
        {
            int len = (int)(args[2].AsInteger() ?? 0);
            return new DbValue.Text(t.Value.Substring(from, Math.Min(len, t.Value.Length - from)));
        }
        return new DbValue.Text(t.Value[from..]);
    }

    private static DbValue EvalConcatFn(DbValue[] args)
    {
        var sb = new StringBuilder();
        foreach (var a in args)
        {
            if (a.IsNull) continue;
            sb.Append(a.AsString() ?? a.ToString());
        }
        return new DbValue.Text(sb.ToString());
    }

    private static DbValue EvalReplace(DbValue[] args)
    {
        if (args.Length != 3) throw ArgCountError("REPLACE", 3, args.Length);
        if (args[0].IsNull) return DbValue.Null.Instance;
        if (args[0] is not DbValue.Text src)
            throw TypeMismatch("string", args[0]);
        var from = args[1].AsString() ?? string.Empty;
        var to = args[2].AsString() ?? string.Empty;
        return new DbValue.Text(src.Value.Replace(from, to, StringComparison.Ordinal));
    }

    private static DbValue EvalNullIf(DbValue[] args)
    {
        if (args.Length != 2) throw ArgCountError("NULLIF", 2, args.Length);
        return (args[0].IsNull || CompareDbValues(args[0], args[1]) == 0)
            ? DbValue.Null.Instance
            : args[0];
    }

    private static DbValue EvalPow(DbValue[] args)
    {
        if (args.Length != 2) throw ArgCountError("POWER", 2, args.Length);
        double? b = args[0].AsFloat(), e = args[1].AsFloat();
        if (b is null || e is null) return DbValue.Null.Instance;
        return new DbValue.Float(Math.Pow(b.Value, e.Value));
    }

    private static DbValue EvalExp(DbValue[] args)
    {
        RequireOne("EXP", args, out var val);
        if (val is null) return DbValue.Null.Instance;
        double? d = val.AsFloat();
        return d.HasValue ? new DbValue.Float(Math.Exp(d.Value)) : throw TypeMismatch("numeric", val);
    }

    private static DbValue EvalLog(DbValue[] args)
    {
        RequireOne("LOG", args, out var val);
        if (val is null) return DbValue.Null.Instance;
        double? d = val.AsFloat();
        return d.HasValue ? new DbValue.Float(Math.Log(d.Value)) : throw TypeMismatch("numeric", val);
    }

    private static DbValue EvalSign(DbValue[] args)
    {
        RequireOne("SIGN", args, out var val);
        if (val is null) return DbValue.Null.Instance;
        return val switch
        {
            DbValue.Integer i => new DbValue.Integer(Math.Sign(i.Value)),
            DbValue.Float f => new DbValue.Integer(Math.Sign(f.Value)),
            _ => throw TypeMismatch("numeric", val)
        };
    }

    // ── CASE expression ───────────────────────────────────────────────────────

    private static DbValue EvalCase(
        Expr? operand,
        IReadOnlyList<(Expr Condition, Expr Result)> conditions,
        Expr? elseResult,
        DbRow row)
    {
        if (operand is not null)
        {
            var opVal = Eval(operand, row);
            foreach (var (cond, result) in conditions)
            {
                var condVal = Eval(cond, row);
                if (!opVal.IsNull && !condVal.IsNull && CompareDbValues(opVal, condVal) == 0)
                    return Eval(result, row);
            }
        }
        else
        {
            foreach (var (cond, result) in conditions)
            {
                var condVal = Eval(cond, row);
                if (condVal.AsBool() == true)
                    return Eval(result, row);
            }
        }
        return elseResult is not null ? Eval(elseResult, row) : DbValue.Null.Instance;
    }

    // ── CAST ──────────────────────────────────────────────────────────────────

    private static DbValue EvalCast(Expr inner, DataType dataType, DbRow row)
    {
        var val = Eval(inner, row);
        if (val.IsNull) return DbValue.Null.Instance;

        return dataType switch
        {
            DataType.Boolean => CastToBool(val),
            DataType.Integer or DataType.BigInt or DataType.SmallInt
            or DataType.TinyInt or DataType.MediumInt => CastToInteger(val),
            DataType.Float or DataType.Double => CastToFloat(val),
            DataType.Varchar or DataType.Char or DataType.TinyText
            or DataType.MediumText or DataType.LongText => new DbValue.Text(val.ToString()!),
            DataType.Decimal => CastToDecimal(val),
            _ => throw new ExecutorException(ExecutorErrorKind.EvalError,
                $"Unsupported CAST target: {dataType.GetType().Name}")
        };
    }

    private static DbValue CastToBool(DbValue val) =>
        val switch
        {
            DbValue.Boolean => val,
            DbValue.Integer i => new DbValue.Boolean(i.Value != 0),
            DbValue.Text t => new DbValue.Boolean(
                t.Value.Equals("true", StringComparison.OrdinalIgnoreCase)
                || t.Value == "t"
                || t.Value == "1"),
            _ => throw TypeMismatch("boolean", val)
        };

    private static DbValue CastToInteger(DbValue val) =>
        val switch
        {
            DbValue.Integer => val,
            DbValue.Float f => new DbValue.Integer((long)f.Value),
            DbValue.Text t when long.TryParse(t.Value, out var n) => new DbValue.Integer(n),
            DbValue.Text t => throw new ExecutorException(ExecutorErrorKind.EvalError,
                $"Cannot cast '{t.Value}' to integer"),
            DbValue.Boolean b => new DbValue.Integer(b.Value ? 1 : 0),
            _ => throw TypeMismatch("integer", val)
        };

    private static DbValue CastToFloat(DbValue val) =>
        val switch
        {
            DbValue.Float => val,
            DbValue.Integer i => new DbValue.Float(i.Value),
            DbValue.Text t when double.TryParse(t.Value,
                System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var d)
                => new DbValue.Float(d),
            DbValue.Text t => throw new ExecutorException(ExecutorErrorKind.EvalError,
                $"Cannot cast '{t.Value}' to float"),
            _ => throw TypeMismatch("float", val)
        };

    private static DbValue CastToDecimal(DbValue val) =>
        val switch
        {
            DbValue.Decimal => val,
            DbValue.Integer i => new DbValue.Decimal(i.Value),
            DbValue.Float f => new DbValue.Decimal((decimal)f.Value),
            DbValue.Text t when decimal.TryParse(t.Value,
                System.Globalization.NumberStyles.Any,
                System.Globalization.CultureInfo.InvariantCulture, out var d)
                => new DbValue.Decimal(d),
            DbValue.Text t => throw new ExecutorException(ExecutorErrorKind.EvalError,
                $"Cannot cast '{t.Value}' to decimal"),
            _ => throw TypeMismatch("decimal", val)
        };

    // ── BETWEEN ───────────────────────────────────────────────────────────────

    private static DbValue EvalBetween(Expr inner, Expr low, Expr high, bool negated, DbRow row)
    {
        var val = Eval(inner, row);
        var lowVal = Eval(low, row);
        var highVal = Eval(high, row);

        if (val.IsNull || lowVal.IsNull || highVal.IsNull)
            return DbValue.Null.Instance;

        var cmpLow = CompareDbValues(val, lowVal);
        var cmpHigh = CompareDbValues(val, highVal);
        bool isBetween = cmpLow >= 0 && cmpHigh <= 0;
        return new DbValue.Boolean(negated ? !isBetween : isBetween);
    }

    // ── IN list ───────────────────────────────────────────────────────────────

    private static DbValue EvalInList(Expr inner, IReadOnlyList<Expr> list, bool negated, DbRow row)
    {
        var val = Eval(inner, row);
        if (val.IsNull) return DbValue.Null.Instance;

        foreach (var itemExpr in list)
        {
            var itemVal = Eval(itemExpr, row);
            if (!itemVal.IsNull && CompareDbValues(val, itemVal) == 0)
                return new DbValue.Boolean(!negated);
        }
        return new DbValue.Boolean(negated);
    }

    // ── IS NULL ───────────────────────────────────────────────────────────────

    private static DbValue EvalIsNull(Expr inner, bool negated, DbRow row)
    {
        var val = Eval(inner, row);
        return new DbValue.Boolean(negated ? !val.IsNull : val.IsNull);
    }

    // ── SUBSTRING (Expr.Substring) ────────────────────────────────────────────

    private static DbValue EvalSubstring(Expr inner, Expr? fromPos, Expr? len, DbRow row)
    {
        var val = Eval(inner, row);
        if (val.IsNull) return DbValue.Null.Instance;
        if (val is not DbValue.Text t) throw TypeMismatch("string", val);

        int from = fromPos is not null
            ? (int)(Eval(fromPos, row).AsInteger() ?? 1) - 1
            : 0;
        if (from < 0) from = 0;
        if (from >= t.Value.Length) return new DbValue.Text(string.Empty);

        if (len is not null)
        {
            var lenVal = Eval(len, row);
            if (!lenVal.IsNull)
            {
                int l = (int)(lenVal.AsInteger() ?? 0);
                return new DbValue.Text(t.Value.Substring(from, Math.Min(l, t.Value.Length - from)));
            }
        }
        return new DbValue.Text(t.Value[from..]);
    }

    // ── TRIM (Expr.Trim) ──────────────────────────────────────────────────────

    private static DbValue EvalTrim(Expr inner, TrimWhereField? trimWhere, Expr? trimWhat, DbRow row)
    {
        var val = Eval(inner, row);
        if (val.IsNull) return DbValue.Null.Instance;
        if (val is not DbValue.Text t) throw TypeMismatch("string", val);

        char[]? chars = null;
        if (trimWhat is not null)
        {
            var what = Eval(trimWhat, row);
            if (what is DbValue.Text tw) chars = tw.Value.ToCharArray();
        }

        string result = trimWhere switch
        {
            TrimWhereField.Leading => chars is null ? t.Value.TrimStart() : t.Value.TrimStart(chars),
            TrimWhereField.Trailing => chars is null ? t.Value.TrimEnd() : t.Value.TrimEnd(chars),
            _ => chars is null ? t.Value.Trim() : t.Value.Trim(chars)
        };
        return new DbValue.Text(result);
    }

    // ── POSITION ─────────────────────────────────────────────────────────────

    private static DbValue EvalPosition(Expr substr, Expr inExpr, DbRow row)
    {
        var subVal = Eval(substr, row);
        var inVal = Eval(inExpr, row);
        if (subVal.IsNull || inVal.IsNull) return DbValue.Null.Instance;
        if (subVal is not DbValue.Text sub || inVal is not DbValue.Text src)
            throw TypeMismatch("string", subVal);

        int idx = src.Value.IndexOf(sub.Value, StringComparison.Ordinal);
        return new DbValue.Integer(idx < 0 ? 0 : idx + 1);
    }

    // ── Comparison helper ─────────────────────────────────────────────────────

    /// <summary>
    /// Compares two non-null DbValues. Returns negative/zero/positive like IComparable.
    /// </summary>
    public static int CompareDbValues(DbValue a, DbValue b)
    {
        if (a.IsNull && b.IsNull) return 0;
        if (a.IsNull) return -1;
        if (b.IsNull) return 1;

        return (a, b) switch
        {
            (DbValue.Integer ai, DbValue.Integer bi) => ai.Value.CompareTo(bi.Value),
            (DbValue.Float af, DbValue.Float bf) => af.Value.CompareTo(bf.Value),
            (DbValue.Integer ai, DbValue.Float bf) => ((double)ai.Value).CompareTo(bf.Value),
            (DbValue.Float af, DbValue.Integer bi) => af.Value.CompareTo((double)bi.Value),
            (DbValue.Text at, DbValue.Text bt) =>
                string.Compare(at.Value, bt.Value, StringComparison.Ordinal),
            (DbValue.Boolean ab, DbValue.Boolean bb) => ab.Value.CompareTo(bb.Value),
            (DbValue.Decimal ad, DbValue.Decimal bd) => ad.Value.CompareTo(bd.Value),
            _ => throw new ExecutorException(ExecutorErrorKind.TypeMismatch,
                $"Cannot compare {a} and {b}")
        };
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void RequireOne(string fn, DbValue[] args, out DbValue? val)
    {
        if (args.Length != 1)
            throw new ExecutorException(ExecutorErrorKind.EvalError, $"{fn} requires 1 argument");
        val = args[0].IsNull ? null : args[0];
    }

    private static void RequireOneString(string fn, DbValue[] args, out string? val)
    {
        RequireOne(fn, args, out var v);
        if (v is null) { val = null; return; }
        if (v is DbValue.Text t) { val = t.Value; return; }
        throw TypeMismatch("string", v);
    }

    private static ExecutorException TypeMismatch(string expected, params DbValue[] got) =>
        new(ExecutorErrorKind.TypeMismatch,
            $"Type mismatch: expected {expected}, got {string.Join(", ", got.Select(v => v.GetType().Name))}");

    private static ExecutorException ArgCountError(string fn, int expected, int got) =>
        new(ExecutorErrorKind.EvalError, $"{fn} requires {expected} argument(s), got {got}");
}
