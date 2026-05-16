# Refactoring Playbook

This folder contains practical guidance and reusable prompts to scale refactoring in small, reviewable pull requests.

## Goals

- Keep each PR small and focused.
- Reduce maintenance risk while refactoring.
- Ensure predictable review and merge flow.
- Avoid large "all-at-once" refactoring batches.

## Documents

- [REFACTORING_ROADMAP.md](./REFACTORING_ROADMAP.md) - staged execution model and PR sizing rules.
- [PROMPT_TEMPLATES.md](./PROMPT_TEMPLATES.md) - copy/paste prompts for planning and executing small refactoring PRs.

## How to Use

1. Start with the roadmap to select the next stage.
2. Pick the appropriate prompt template for the PR type.
3. Keep scope limited to one context per PR.
4. Validate acceptance criteria before opening the PR.
