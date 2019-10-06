import pendulum

from monitoring_etl_flow import cmd


class TestJournalCTLCommand:
    def test_command_defaults_to_48_hours(self):
        now = pendulum.now("utc")
        out = cmd.run([])

        assert isinstance(out, str)
        assert now.add(hours=-48).strftime("%Y-%m-%d") in out
