namespace AeternumDB.Core.Executor;

using AeternumDB.Core.Abstractions.Executor;
using AeternumDB.Core.Errors;

/// <summary>
/// Execution context holding table provider, ACL, and object-ID generator.
/// Implements <see cref="IExecutionContext"/> for use with <see cref="IExecutionPlan"/> operators.
/// </summary>
public sealed class ExecutionContext : IExecutionContext
{
    private readonly IObjIdGenerator _objIdGen;

    public ITableProvider TableProvider { get; }
    public Acl Acl { get; }
    public string CurrentUser { get; }

    public ExecutionContext(
        ITableProvider tableProvider,
        Acl acl,
        IObjIdGenerator objIdGen,
        string currentUser)
    {
        TableProvider = tableProvider;
        Acl = acl;
        _objIdGen = objIdGen;
        CurrentUser = currentUser;
    }

    #region Public Methods

    public long NextObjectId() => _objIdGen.NextId();

    public void GrantPrivilege(string user, string obj, string privilege) =>
        Acl.Grant(user, obj, privilege);

    public void RevokePrivilege(string user, string obj, string privilege) =>
        Acl.Revoke(user, obj, privilege);

    /// <summary>Throws <see cref="ExecutorException"/> if the current user lacks the specified privilege.</summary>
    public void CheckPrivilege(string obj, string privilege)
    {
        if (!Acl.Check(CurrentUser, obj, privilege))
        {
            throw new ExecutorException(
                ExecutorErrorKind.PermissionDenied,
                $"User {CurrentUser} lacks {privilege} privilege on {obj}");
        }
    }

    /// <summary>Factory for creating a default test context backed by an in-memory provider.</summary>
    public static ExecutionContext CreateForTesting() =>
        new(new InMemoryTableProvider(), new Acl(), new AtomicIdGenerator(), "test_user");

    #endregion Public Methods
}
