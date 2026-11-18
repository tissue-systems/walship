# Changelog

## Unreleased

The receiver checks every batch before applying any of it. Each frame has to
name a real page, carry the same salt as the rest of the batch, and chain its
checksum from the frame before it the way SQLite wrote it, and a commit frame
can't give a database size smaller than a page its transaction wrote. A batch
that continues the replica is also checked against the last frame applied.
A failure is a 400 and the replica is left as it was. A batch from an older
generation is still answered with the replica's position, before its frames
are checked, so the shipper knows to send a base.

When committing a transaction fails partway through a batch, the replica's
frame count is the last commit that did land, not the one before the batch.

The base and frames routes take at most 2 and 8 requests at once, set with
`Routes::concurrency`. A request past the limit gets a 503 before its body is
read, which the shipper already retries.

## 0.1.1 (2026-10-28)

Fixes the docs.rs build. Nightly rustdoc folded the `doc_auto_cfg` feature
into `doc_cfg`, so 0.1.0 has no documentation there.

The stress example could stop its shipper before the writer's last commit
and then report a replica one transaction short. The library is unchanged.

## 0.1.0 (2026-10-28)

First release. The shipper, receiver and WAL code come out of a hosted
database service, where they have replicated customer databases in
production since September 2026.
