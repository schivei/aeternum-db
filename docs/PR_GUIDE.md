# PR Development Guide

This guide helps contributors implement PRs from the implementation plan effectively.

## Before Starting a PR

### 1. Check Prerequisites
- [ ] Review the [Implementation Plan](./IMPLEMENTATION_PLAN.md)
- [ ] Read the specific phase plan (e.g., [Phase 1 PRs](./phase1-prs.md))
- [ ] Verify all dependency PRs are merged
- [ ] Check no one else is working on it (GitHub issues/PRs)
- [ ] Set up development environment

### 2. Claim the PR
1. Create a GitHub issue: "Implement PR X.Y: [Title]"
2. Assign yourself to the issue
3. Comment your estimated start/end dates
4. Link to the planning document

### 3. Create Your Branch
```bash
git checkout main
git pull origin main
git checkout -b feature/prX.Y-short-description
```

Branch naming convention:
- `feature/pr1.1-storage-engine`
- `feature/pr2.3-extension-registry`
- `bugfix/pr1.5-query-executor-crash`

---

## During Development

### 1. Follow the Spec
Each PR plan includes:
- **Objectives:** What to build
- **Implementation Details:** Files to create/modify
- **Key Features:** Required functionality
- **Tests Required:** What to test
- **Acceptance Criteria:** Definition of done

**Stick to the spec.** If you need to deviate:
1. Document why in the PR description
2. Update the planning doc
3. Discuss in PR review

### 2. Code Standards

#### Rust Code Style
```rust
// Use rustfmt
cargo fmt

// Use clippy
cargo clippy -- -D warnings

// Add module documentation
//! Module for storage engine implementation.
//!
//! This module provides...

/// Creates a new storage engine.
///
/// # Arguments
/// * `config` - Configuration options
///
/// # Examples
/// ```
/// let engine = StorageEngine::new(config);
/// ```
pub fn new(config: Config) -> Result<Self, Error> {
    // ...
}
```

#### Testing Requirements
- **Unit tests:** Test individual functions
- **Integration tests:** Test component interactions
- **Performance tests:** Benchmark critical paths
- **Coverage:** Aim for >80% (checked by CI)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_engine_creation() {
        let config = Config::default();
        let engine = StorageEngine::new(config);
        assert!(engine.is_ok());
    }

    #[tokio::test]
    async fn test_async_operations() {
        // ...
    }
}
```

#### Documentation Requirements
- Public APIs must have doc comments
- Complex algorithms need explanation
- Update relevant markdown docs
- Add examples where helpful

### 3. Commit Messages

Use conventional commits:
```
type(scope): short description

Longer description if needed.

- Bullet points for details
- Reference issues: Fixes #123

Related to PR plan: docs/phase1-prs.md, PR 1.1
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `test`: Adding tests
- `refactor`: Code restructuring
- `perf`: Performance improvement
- `chore`: Maintenance

Examples:
```
feat(storage): implement buffer pool manager

Add LRU eviction policy for buffer pool with configurable
size and pin/unpin mechanism.

- Add BufferPool struct
- Implement LRU eviction
- Add concurrent access support
- Include unit tests

Related to PR plan: docs/phase1-prs.md, PR 1.1
```

### 4. Running Tests Locally

```bash
# Run all tests
cargo test

# Run specific module tests
cargo test storage::

# Run with output
cargo test -- --nocapture

# Run benchmarks
cargo bench

# Check coverage (requires tarpaulin)
cargo tarpaulin --out Html
```

### 5. Performance Validation

If PR includes performance requirements:
```bash
# Run benchmarks
cargo bench --bench storage_bench

# Compare with baseline
# Save baseline first time
cargo bench -- --save-baseline main

# Compare after changes
cargo bench -- --baseline main
```

Document performance in PR:
- Before/after metrics
- Comparison to targets
- Any degradation explained

---

## Submitting the PR

### 1. Pre-Submission Checklist
- [ ] All tests pass locally
- [ ] Code formatted (`cargo fmt`)
- [ ] No clippy warnings (`cargo clippy`)
- [ ] Documentation updated
- [ ] Performance targets met (if applicable)
- [ ] Commit messages follow convention
- [ ] Branch rebased on latest main

### 2. PR Description Template

```markdown
## PR X.Y: [Title from plan]

### Objectives
[Copy from plan or summarize]

### Implementation
[What you built and how]

### Changes
- Created/modified files list
- Key architectural decisions
- Any deviations from plan

### Testing
- Unit tests: [count] added
- Integration tests: [count] added
- All tests passing: ✅
- Coverage: [percentage]%

### Performance
[If applicable]
- Metric: [value] (target: [target])
- Benchmarks: [link to results]

### Documentation
- [X] Code documented
- [X] API docs updated
- [X] User guide updated
- [X] Examples added

### Checklist
- [X] Follows code standards
- [X] Tests comprehensive
- [X] Documentation complete
- [X] Performance validated
- [X] No breaking changes (or documented)

### Related
- Planning doc: [link]
- Depends on: PR #X (if any)
- Related issues: #Y

### Screenshots
[If UI changes]
```

### 3. Submit PR
```bash
git push origin feature/prX.Y-short-description
```

Create PR on GitHub:
- Title: "PR X.Y: [Title]"
- Description: Use template above
- Labels: `phase-N`, `priority-critical/high/medium`
- Reviewers: Request reviews
- Link issue: "Closes #123"

---

## During Review

### 1. Respond to Feedback
- Address all comments
- Ask for clarification if needed
- Push updates to same branch
- Mark resolved conversations

### 2. CI/CD Checks
GitHub Actions runs:
- `cargo test` on Linux, Windows, macOS
- `cargo clippy`
- `cargo fmt --check`
- Coverage report
- Performance benchmarks (for perf PRs)

All must pass before merge.

### 3. Update Documentation
If reviewers request changes to docs:
- Update inline comments
- Update markdown docs
- Add examples if requested

---

## After Merge

### 1. Cleanup
```bash
git checkout main
git pull origin main
git branch -d feature/prX.Y-short-description
```

### 2. Verify Deployment
- Check main branch builds
- Verify tests pass in CI
- Check documentation deployed

### 3. Update Planning
If you made changes to the plan:
- Update the phase plan doc
- Note what changed and why
- Submit follow-up PR if needed

---

## Common Issues

### "Tests fail in CI but pass locally"
- Ensure you're on latest main
- Check for race conditions
- Verify environment variables
- Look at CI logs carefully

### "Performance below target"
- Profile the code
- Check for obvious inefficiencies
- Discuss in PR - may need optimization PR
- Document as known issue if acceptable

### "Breaking change required"
- Discuss with maintainers first
- Document migration path
- Update major version
- Add to changelog

### "PR too large"
- Split into smaller PRs
- Update planning doc
- Submit sequentially

---

## Getting Help

### Resources
- **Documentation:** `/docs` folder
- **Examples:** `/examples` folder (coming soon)
- **Tests:** Existing tests for patterns
- **Discord:** [To be set up]

### Asking Questions
1. Check planning docs first
2. Search existing issues/PRs
3. Ask in PR comments
4. Open a discussion thread
5. Ping maintainers if urgent

### Reporting Issues
If you find issues in the plan:
1. Open issue: "Planning: [problem]"
2. Tag with `planning` label
3. Reference specific PR plan
4. Suggest fix if possible

---

## Tips for Success

### Do's ✅
- Read the full PR plan before starting
- Write tests first (TDD)
- Commit frequently with good messages
- Ask questions early
- Document as you go
- Review your own PR before submitting

### Don'ts ❌
- Don't skip tests
- Don't ignore clippy warnings
- Don't submit without documentation
- Don't make unrelated changes
- Don't merge without review
- Don't break existing tests

### Performance Tips
- Benchmark critical paths
- Use appropriate data structures
- Avoid unnecessary allocations
- Profile before optimizing
- Document performance characteristics

### Documentation Tips
- Explain *why*, not just *what*
- Include examples
- Link to related docs
- Keep it up to date
- Use diagrams when helpful

---

## Example Workflow

Here's a complete example of implementing PR 1.1:

```bash
# 1. Setup
git checkout main
git pull origin main
git checkout -b feature/pr1.1-storage-engine

# 2. Create issue
# On GitHub: "Implement PR 1.1: Storage Engine"

# 3. Implement
# ... write code ...

# 4. Test locally
cargo test storage::
cargo fmt
cargo clippy

# 5. Commit
git add core/src/storage/
git commit -m "feat(storage): implement basic storage engine

Add page-based storage with buffer pool and file manager.

- Implement Page struct with 4KB pages
- Add BufferPool with LRU eviction
- Create FileManager for disk I/O
- Include comprehensive unit tests

Related to PR plan: docs/phase1-prs.md, PR 1.1"

# 6. Push
git push origin feature/pr1.1-storage-engine

# 7. Create PR on GitHub with template

# 8. Address review feedback
# ... make changes ...
git add .
git commit -m "fix(storage): address review feedback"
git push

# 9. After merge
git checkout main
git pull origin main
git branch -d feature/pr1.1-storage-engine
```

---

## Conclusion

Following this guide ensures:
- Consistent code quality
- Comprehensive testing
- Good documentation
- Smooth review process
- Successful PR merges

**Remember:** Quality over speed. Take time to do it right.

---

**Related Documents:**
- [Implementation Plan](./IMPLEMENTATION_PLAN.md)
- [Phase 1 PRs](./phase1-prs.md)
- [Phase 2 PRs](./phase2-prs.md)
- [Contributing Guide](../CONTRIBUTING.md)
