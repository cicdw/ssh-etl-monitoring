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
        record = """{ "__CURSOR" : "s=2353620f1f8a44f5bc366f5c1b3ece94;i=2ba0;b=9ec65c10cb984e45b8e37c2735a2f110;m=79ba7135c;t=59436d286918b;x=679dfd1f9a2f4acd", "__REALTIME_TIMESTAMP" : "1570338064732555", "__MONOTONIC_TIMESTAMP" : "32676189020", "_BOOT_ID" : "9ec65c10cb984e45b8e37c2735a2f110", "_MACHINE_ID" : "d7a4f7c9f67846cd92e1df36f2182db9", "PRIORITY" : "6", "_UID" : "0", "_GID" : "0", "_SELINUX_CONTEXT" : "unconfined\n", "_SYSTEMD_SLICE" : "system.slice", "_CAP_EFFECTIVE" : "3fffffffff", "SYSLOG_FACILITY" : "4", "_HOSTNAME" : "ssh-monitoring-ex", "_TRANSPORT" : "syslog", "SYSLOG_IDENTIFIER" : "sshd", "_COMM" : "sshd", "_EXE" : "/usr/sbin/sshd", "_SYSTEMD_CGROUP" : "/system.slice/ssh.service", "_SYSTEMD_UNIT" : "ssh.service", "_SYSTEMD_INVOCATION_ID" : "dc8cb482ece0451fba37a8da6b0587d9", "_CMDLINE" : "sshd: [accepted]", "SYSLOG_PID" : "10741", "MESSAGE" : "Invalid user Boutique@123 from 162.243.165.39 port 43146", "_PID" : "10741", "_SOURCE_REALTIME_TIMESTAMP" : "1570338064732199" }"""
        rows = transform.run([record])
        assert len(rows) == 1
        row = rows[0]

        assert row["username"] == "Boutique@123"
        assert row["port"] == 43146
