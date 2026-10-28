# Security policy

## Reporting a vulnerability

Please don't open a public issue for a security problem. Email
[security@tissue.systems](mailto:security@tissue.systems) with what you found
and the steps to reproduce it. A small program or a test case that shows the
problem is the most useful thing you can send.

We'll reply to say we've got the report, keep you posted while we work on a
fix, and credit you in the release notes unless you'd rather we didn't. Please
give us a reasonable amount of time to ship the fix before you publish.

## Supported versions

walship is pre-1.0. Fixes go into the latest 0.x release only.

## What's in scope

The receiver accepts writes over HTTP, so most of what matters is on that side:

- anything that gets past the bearer token check
- a database name that escapes the replica directory
- a request that makes the receiver write a replica that doesn't match what the
  primary sent, without reporting an error
- a request that exhausts memory or disk past the documented limits (bases are
  capped by `Routes::max_base_bytes`, frame batches by a fixed body limit)

Running the receiver without a token, or across a network you don't trust
without TLS in front of it, is a deployment choice the docs warn about, not a
vulnerability in walship. The same goes for bugs in libSQL or SQLite
themselves; report those upstream. If you're not sure, send it anyway.
