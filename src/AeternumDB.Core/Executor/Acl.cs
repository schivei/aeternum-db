namespace AeternumDB.Core.Executor;

/// <summary>Access control list for privilege checking.</summary>
public sealed class Acl
{
    private readonly Dictionary<(string User, string Obj), HashSet<string>> _grants = new();

    #region Public Methods

    public void Grant(string user, string obj, string privilege)
    {
        var key = (user, obj);
        if (!_grants.TryGetValue(key, out var set))
        {
            set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _grants[key] = set;
        }

        set.Add(privilege);
    }

    public void Revoke(string user, string obj, string privilege)
    {
        var key = (user, obj);
        if (_grants.TryGetValue(key, out var set))
            set.Remove(privilege);
    }

    public bool Check(string user, string obj, string privilege)
    {
        var key = (user, obj);
        return _grants.TryGetValue(key, out var set)
            && set.Contains(privilege, StringComparer.OrdinalIgnoreCase);
    }

    #endregion Public Methods
}
