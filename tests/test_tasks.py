"""
Tests of custom tasks.
"""
import pendulum
import pytest
from unittest.mock import MagicMock

from monitoring_etl_flow import cmd, collect_stats, insert_rows, transform
from prefect.engine.signals import SKIP


class TestJournalCTLCommand:
    def test_command_defaults_to_48_hours(self):
        now = pendulum.now("utc")
        out = cmd.run([])

        assert isinstance(out, str)
        assert now.add(hours=-48).strftime("%Y-%m-%d") in out

    def test_command_is_responsive_to_passed_date(self):
        now = pendulum.now("utc")
        out = cmd.run([("1986-09-20 03:32:01",)])

        assert '"1986-09-20 03:32:01"' in out


class TestTransformTask:
    def test_transform_runs_with_empty_list(self):
        assert transform.run([]) == []

    def test_transform_converts_journal_record_to_row(self):
        record = '{"MESSAGE": "Invalid user Boutique@123 from 162.243.165.39 port 43146", "__REALTIME_TIMESTAMP": "1570338064732555"}'
        rows = transform.run([record])
        assert len(rows) == 1
        row = rows[0]

        assert row["username"] == "Boutique@123"
        assert row["port"] == 43146
        assert row["timestamp"].startswith("2019-10")

    def test_transform_only_selects_certain_messages(self):
        records = [
            '{"MESSAGE": "Invalid user Boutique@123 from 162.243.165.39 port 43146", "__REALTIME_TIMESTAMP": "1570338064732555"}',
            '{"MESSAGE": "Failed password for root from 49.88.112.74 port 28851 ssh2", "__REALTIME_TIMESTAMP": "1570338064732555"}',
            '{"MESSAGE": "Disconnected from invalid user P@ssword@2012 212.47.238.207 port 40934 [preauth]", "__REALTIME_TIMESTAMP": "1570338064732555"}',
            '{"MESSAGE": "Disconnected from authenticating user root 217.113.28.5 port 50516 [preauth]", "__REALTIME_TIMESTAMP": "1570338064732555"}',
        ]
        rows = transform.run(records)
        assert len(rows) == 1
        row = rows[0]

        assert row["username"] == "Boutique@123"
        assert row["port"] == 43146
        assert row["timestamp"].startswith("2019-10")


class TestInsertScript:
    def test_insert_rows_runs_with_empty_list(self):
        with pytest.raises(SKIP, match="No rows to insert"):
            insert_rows.run([])

    def test_insert_script_correctly_parses_row(self, monkeypatch):
        monkeypatch.setattr("monitoring_etl_flow.sqlite3", MagicMock())
        closing_mock = MagicMock()
        monkeypatch.setattr("monitoring_etl_flow.closing", closing_mock)

        rows = [
            dict(
                username="chris",
                port=22,
                city="Oakland",
                country="USA",
                timestamp="2019-10-15",
                latitude=20.9,
                longitude=42.4,
            )
        ]
        assert insert_rows.run(rows) is None

        args = closing_mock.return_value.__enter__.return_value.executemany.call_args[0]
        assert "INSERT INTO SSHATTEMPTS" in args[0]
        assert args[1] == [("2019-10-15", "chris", 22, "Oakland", "USA", 20.9, 42.4)]


class TestCollectStats:
    def test_collect_stats_skips_for_old_timestamps(self):
        with pytest.raises(SKIP, match="last 24 hours"):
            collect_stats.run(timestamp=pendulum.now("utc").add(hours=-2.1))
