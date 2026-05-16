# Staged Refactoring Roadmap

This roadmap defines how to scale refactoring without creating oversized pull requests.

## Core Constraints

- One strict scope per PR.
- No mixed concerns in the same PR.
- Prefer sequence of small PRs over one large PR.
- Keep behavior stable unless the PR explicitly targets behavior change.

## PR Size Guardrails

- **Small PR (preferred):** up to one context/subcontext, limited file set, straightforward review.
- **Medium PR (exception):** only when a small PR is technically blocked by dependencies.
- **Large PR (disallowed):** split before implementation.

## Stage 0 - Inventory and Prioritization

Objective: map what should be refactored first.

Deliverables:
- List of large files and oversized types.
- List of duplicated logic candidates.
- Prioritized queue by maintenance impact.

Exit criteria:
- Refactoring queue exists and is ordered by risk/impact.

## Stage 1 - File and Type Boundaries

Objective: enforce structural boundaries.

Deliverables:
- One type per file.
- Generic arity filename convention (`Type.cs`, ``Type`1.cs``, ``Type`2.cs``).
- Folder and namespace segmentation by context and subcontext.

Exit criteria:
- Target context follows naming and segmentation conventions.

## Stage 2 - Type Decomposition

Objective: reduce complexity of oversized types.

Deliverables:
- Extract responsibilities into focused types.
- Keep each type in a strict, cohesive scope.

Exit criteria:
- Refactored context has smaller, single-responsibility types.

## Stage 3 - Duplication Reduction

Objective: remove repeated logic while preserving paradigms.

Deliverables:
- Extract shared logic into helpers/extensions.
- Eliminate duplicated logic in the target context.

Exit criteria:
- No relevant duplication remains in the refactored scope.

## Stage 4 - Documentation and Readability

Objective: improve clarity without inline comments.

Deliverables:
- XML documentation where needed.
- Explanatory blocks promoted to methods/helpers/extensions.
- Region organization aligned with repository conventions.

Exit criteria:
- Code is self-explanatory and follows documentation conventions.

## Stage 5 - Stabilization and Next Slice

Objective: close the PR with a clean handoff.

Deliverables:
- Validation results attached to PR.
- Follow-up refactoring slice identified.
- Next small PR scope defined.

Exit criteria:
- Current slice merged or review-ready with next slice prepared.

## Definition of Done for Each Refactoring PR

- Scope is limited to a single, explicit context.
- No unrelated changes.
- Conventions from this repository are followed.
- Reviewers can validate the change without broad cross-module context.
