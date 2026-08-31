## Command(s) implemented

<!-- e.g. XREADGROUP, or the RANK/COUNT options for LPOS -->

## Related issue

<!-- Link the tracking issue, if one exists. If this addresses a 🚫 command, link the
     issue where the case was made and agreed on before implementation started. -->

## Behavioral notes

<!-- Anything a reviewer needs to know about semantics: does this run under the same
     snapshot-isolation guarantee as MULTI/EXEC? Any deviation from stock Redis/Mongo
     behavior? Any options/flags intentionally left unimplemented? -->

## Checklist

- [ ] `cargo fmt` and `cargo clippy` pass locally
- [ ] Unit tests added
- [ ] Deno integration test added/updated (`test/`), if applicable
- [ ] [COMPATIBILITY.md](../COMPATIBILITY.md) row updated (status + notes)