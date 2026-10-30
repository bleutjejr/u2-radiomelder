# u2-radiomelder
U2 radio alert (JOE / Radio 10)

`watcher.py` polls JOE and Radio 10 on their scheduled days and sends an ntfy.sh push when a U2 song plays. GitHub Actions (`.github/workflows/u2-radio-watcher.yml`) keeps that loop running until 5 November 2026.

The ntfy topic is the `NTFY_TOPIC` Actions secret only. It is not stored in this repo.
