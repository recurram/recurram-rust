# Contributing

Thank you for improving the Recurram Rust implementation.

## Scope

This crate implements the Recurram wire format and session-aware encoder/decoder. Keep changes aligned with the normative spec in [recurram/recurram](https://github.com/recurram/recurram).

## Development

Requirements:

- Rust stable (edition 2024)

```bash
cargo test
cargo fmt --all
cargo clippy --all-targets --all-features
```

## Commit Messages

We follow [Conventional Commits](https://www.conventionalcommits.org/).

Use this format:

```text
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

Common types include `feat`, `fix`, `docs`, `refactor`, `test`, `build`, `ci`, and `chore`.

Examples:

- `feat: add trained dictionary batch encoding`
- `fix(codec): reject invalid control stream frames`

Pull requests are checked in CI so every commit in the branch follows the same rules.

## Pull Requests

Use the pull request template and fill in every required section. PR bodies are validated in CI.

## Contribution Checklist

- Tests added or updated for behavior changes
- `cargo test`, `cargo fmt --all`, and `cargo clippy` pass locally
- Spec-relevant behavior is reflected in tests or docs when needed
- Commit messages follow Conventional Commits

By contributing to this repository, you agree that your contribution may be distributed under the MIT license used by the project.
