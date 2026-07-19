---
name: release
description: Cut a new release of trino-rust-client and publish it to crates.io. Use when asked to release a version, bump the version, or publish the crate.
---

# Releasing trino-rust-client

Follow these steps in order. The tag (created by the GitHub Release, **not**
locally) triggers `.github/workflows/crate-release.yaml`, which publishes to
crates.io.

## 1. Decide the version

Pre-1.0 semver: a `## [Unreleased]` section containing any **Breaking:** entry
bumps the **minor** (`0.10 → 0.11`); otherwise bump the **patch**. Read
`CHANGELOG.md` to decide.

## 2. Check EVERY published sub-crate (easy to miss — it broke 0.10.0)

The workspace publishes **two** crates: `trino-rust-client` and
`trino-rust-client-macros` (the macros crate has its **own** version, not
workspace-inherited). If `trino-rust-client-macros/` changed since its last
publish, bump its `version` **and** the `workspace.dependencies.trino-rust-client-macros`
reference — otherwise `katyo/publish-crates` fails the whole release with
`package 'trino-rust-client-macros' modified since 'X'`.

## 3. Prepare the release branch

```bash
git checkout -b release/X.Y.Z
```

- Bump `workspace.package.version` in `Cargo.toml` to `X.Y.Z`.
- Finalize `CHANGELOG.md`: turn `## [Unreleased]` into `## [X.Y.Z] - <date>`
  (leave an empty `[Unreleased]` above it) and add the comparison link at the
  bottom (**no `v` prefix** — tags are `X.Y.Z`, e.g. `compare/0.10.0...0.11.0`).
- Update the version in `README.md` install snippets (there are two).
- If the release has breaking changes, make sure `MIGRATION.md` covers them.
- `cargo check` and `cargo check --features spooling`.
- Commit `release X.Y.Z`, push the branch, open a PR.

## 4. After the PR is merged — publish the release-drafter **draft**

**Do not create a release by hand.** release-drafter maintains a **draft**
GitHub Release (named `X.Y.Z 🌈`) that already lists every merged PR with its
author — `- <title> @<author> (#<n>)` — so contributors are credited
automatically. Publish and lightly edit **that draft** instead.

```bash
gh release list --repo nudibranches-tech/trino-rust-client   # find the draft
```

Then publish it, fixing the tag/title/target and enriching the notes:

```bash
gh release edit <draft-tag> \
  --tag X.Y.Z --target main --title "X.Y.Z 🌈" \
  --notes-file <notes> --draft=false --latest
```

- **Tag: `X.Y.Z`** — no `v` prefix. The draft's version may be wrong (release-drafter
  guesses from `semver:*` labels, which live in the tracking repo, not on these
  PRs) — set it explicitly.
- **Title: `X.Y.Z 🌈`** — keep the rainbow.
- **Notes**: start from the draft's auto-generated `## Changes` (keep the
  per-PR `@author` attribution — the whole point). Trino PRs usually carry no
  `type:*` labels, so release-drafter won't categorise them well; tidy the
  grouping/wording and add a `**Full Changelog**: .../compare/<prev>...<this>`
  link if missing. Preserve the contributor `@mentions`.

Publishing the draft creates the tag, which triggers the publish workflow
(~4 min). Watch it and confirm both crates on crates.io:

```bash
gh run watch <run-id> --exit-status
```

If no draft exists (release-drafter didn't run), fall back to
`gh release create X.Y.Z --target main --title "X.Y.Z 🌈" --notes-file <notes> --latest`,
and add a `## Contributors` section listing the PR authors by hand.

## Notes

- Do **not** create git tags locally — the GitHub Release creates them.
- Git SSH is not configured; use `gh` for remote operations.
- release-drafter maintains a rolling draft named `X.Y.Z 🌈`; that is expected,
  not a stale artifact.
