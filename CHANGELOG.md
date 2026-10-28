# Changelog

## 0.1.1 (2026-10-28)

Fixes the docs.rs build. Nightly rustdoc folded the `doc_auto_cfg` feature
into `doc_cfg`, so 0.1.0 has no documentation there.

The stress example could stop its shipper before the writer's last commit
and then report a replica one transaction short. The library is unchanged.

## 0.1.0 (2026-10-28)

First release. The shipper, receiver and WAL code come out of a hosted
database service, where they have replicated customer databases in
production since September 2026.
