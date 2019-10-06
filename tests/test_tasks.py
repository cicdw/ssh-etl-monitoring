import pendulum

from monitoring_etl_flow import cmd, transform


class TestJournalCTLCommand:
    def test_command_defaults_to_48_hours(self):
        now = pendulum.now("utc")
        out = cmd.run([])

        assert isinstance(out, str)
        assert now.add(hours=-48).strftime("%Y-%m-%d") in out

    def test_command_is_responsive_to_passed_date(self):
        now = pendulum.now("utc")
        out = cmd.run(["1986-09-20 03:32:01"])

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
        assert row["timestamp"].startswith("2019-10-15")
