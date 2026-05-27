# EC2 run archive

Captured outputs from previous SLA test runs on EC2 — kept for reference, not
required to run the tests.

| Folder  | What it contains                                            |
| ------- | ----------------------------------------------------------- |
| `run-1` | `logs/sla_20260521T122044Z*.log` (Nominis)                  |
| `run-2` | `logs/sla_20260521T161851Z*.log` + `g46rau.tgz` (full run)  |
| `run-3` | `logs/sla_20260521T180840Z*.log` + `1vpd44.tgz` (full run)  |

The `*.tgz` files are full log bundles uploaded from EC2 via `litterbox`.

New runs go in `../logs/` (gitignored).
