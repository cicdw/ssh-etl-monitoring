# ssh-etl-monitoring

This repo provides a fully realized [Prefect](https://github.com/PrefectHQ/prefect) Flow for monitoring SSH attempts on an Ubuntu server.

### monitoring_etl_flow.py

This flow runs every two hours, and collects all invalid ssh attempts in a local SQLite3 database.
