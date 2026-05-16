# Refactoring Prompt Templates

Use these prompts to keep refactoring work incremental and maintainable.

## 1) Refactoring Slice Planner Prompt

```text
Create a refactoring plan for a single small PR in the <context> area.

Constraints:
- One strict scope only.
- No unrelated changes.
- Keep behavior unchanged.
- Keep PR small and reviewable.

Repository conventions to enforce:
- One type per file.
- Generic arity file naming convention.
- Small, focused types with strict responsibility.
- Folder/namespace segmentation by context and subcontext.
- Prefer helpers/extensions to reduce duplication.
- Use regions for organization.
- No inline comments; use XML docs and method extraction for explanations.
- Apply DRY, YAGNI, KISS, SOLID, and 12-Factor principles.

Output required:
1. PR objective
2. In-scope files
3. Out-of-scope items
4. Acceptance criteria
5. Risks and rollback notes
6. Suggested next slice
```

## 2) Small Refactoring Implementation Prompt

```text
Implement exactly one small refactoring slice in <context>.

Hard limits:
- Do not expand scope beyond listed files.
- Do not mix feature work with refactoring.
- Keep behavior compatibility.

Required actions:
- Apply one-type-per-file and naming conventions where touched.
- Decompose oversized types only within this slice.
- Extract duplication to helpers/extensions when safe.
- Replace explanatory inline comments with XML docs or extracted methods.

Output required:
1. Summary of changes
2. Files changed
3. Validation results
4. What remains for next PR
```

## 3) PR Scope Gate Prompt

```text
Review this proposed refactoring PR scope and decide if it is small enough.

Inputs:
- PR objective
- Files planned
- Context/subcontext impacted
- Estimated review complexity

Decision rules:
- Approve only if the PR has one strict scope and low review complexity.
- If scope is too large, split into 2-3 smaller PR slices.

Output required:
1. Scope decision (approve/split)
2. Reasoning
3. If split: exact slice boundaries per PR
```

## 4) Follow-Up Slice Prompt

```text
Based on the completed refactoring PR, define the next smallest safe slice.

Requirements:
- Keep continuity with previous changes.
- Avoid touching unrelated modules.
- Prefer stability and readability gains first.

Output required:
1. Next PR objective
2. Exact files or folder target
3. Expected maintainability gain
4. Acceptance criteria
```

## 5) Reviewer Checklist Prompt

```text
Act as a reviewer focused on refactoring quality and scope control.

Validate:
- Single-scope PR
- No unrelated edits
- One type per file in touched areas
- Correct naming/namespace segmentation
- Reduced duplication through helpers/extensions
- No inline comments added
- XML docs/method extraction used where explanation is needed
- Readability principles applied (DRY, YAGNI, KISS, SOLID, 12-Factor)

Output required:
1. Pass/fail per checklist item
2. Blocking issues
3. Merge recommendation
```

