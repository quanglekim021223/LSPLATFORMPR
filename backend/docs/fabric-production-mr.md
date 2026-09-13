# Draft MR: Run incremental vendor ingestion into Fabric Bronze

The existing timer pulls vendor data but does not publish the prepared Bronze
tables. Its local SQLite checkpoint does not survive deployment, and advancing
watermarks before publishing risks missing data after a publish failure.

This change connects the existing vendor services to a Managed Identity Fabric
publisher. Each vendor uses a leased Azure Blob state, prepares source-shaped
Parquet locally, persists the pending batch, appends Delta tables with stable
transaction generations, then commits its checkpoint after all tables succeed.
Retry resumes a pending batch without downloading the vendor data again or
duplicating the same Delta transaction. Bronze keeps business duplicates and
changed record versions, with ingestion time, run, vendor and domain metadata on
each row; the bootstrap mapper is shared with the runtime. Checkpoints remain in
the separate Blob state rather than becoming Bronze control tables.

The Fabric path disables periodic full/weekly history resync while preserving
daily overlap. Failed/partial vendor summaries fail the timer, and the package
includes the required Delta dependencies. This change does not alter the
repository CI/CD pipeline.

## Validation

- Local full pytest suite: 266 passed; toàn bộ backend pass Ruff; strict mypy:
  101 source files.
- Wheel built and required runtime modules verified; Python 3.11 syntax checked
  locally (not a live GitLab pipeline or Linux deployment).
- Actual local Delta commits and retries after lost publish acknowledgement.
- Durable checkpoint failure, no-change batch, additive schema, raw count/checksum.
- External Bronze DELETE recovery on an empty incremental batch with 50,000 rows.
- Monthly boundary: all four history planners keep incremental ranges in Fabric mode.
- SkillUp assessment splits truncated windows; incomplete windows cannot advance
  the checkpoint. Harvard scans available SFTP files and skips unchanged files.
- Seed rejects partial runs and missing full-sync history state.
- No live production deployment, identity/RBAC/network validation or bootstrap-state
  migration performed by this MR.

## Rollout requirements

- Supply production target and state-storage settings; do not reuse DEV IDs silently.
- Provision Managed Identity permissions, container, schema, trusted SFTP host keys
  and vendor network access.
- Seed audited published checkpoints; bootstrap batch databases may require a
  separate manifest-based migration to contain all incremental scopes.
- Deploy with timer disabled, run authenticated HTTP smoke test twice, validate
  tables and state, then enable the schedule.

See [fabric-production.md](fabric-production.md) for settings and commands.
