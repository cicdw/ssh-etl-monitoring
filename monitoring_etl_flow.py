import geoip2.database as geo_db
import json
import os
import pendulum
import re
import sqlite3
from datetime import datetime, timedelta

from prefect import task, Flow, Parameter
from prefect.engine.signals import SKIP
from prefect.schedules import IntervalSchedule
from prefect.tasks.database.sqlite import SQLiteScript, SQLiteQuery
from prefect.tasks.notifications.email_task import EmailTask
from prefect.tasks.shell import ShellTask

## preliminary setup
## - create database
## - determine last seen date
create_script = "CREATE TABLE IF NOT EXISTS SSHATTEMPTS (timestamp TEXT, username TEXT, port INTEGER, city TEXT, country TEXT, latitude REAL, longitude REAL)"
create_table = SQLiteScript(
    db="ssh.db", script=create_script, name="Create Database and Table"
)
last_date = SQLiteQuery(
    db="ssh.db",
    query="SELECT timestamp FROM SSHATTEMPTS ORDER BY timestamp DESC Limit 1",
    name="Last Seen Date",
)

## data manipulation
## - extract logs since last collected
## - format / extract relevant data fields
## - retrieve location information
## - place into database
@task(name="Format Command")
def cmd(last_date):
    """
    Based on the last available date in the database, creates the appropriate
    journalctl command to collect all sshd logs since the last seen date.
    """
    if not last_date:
        since = pendulum.now("utc").add(hours=-48).strftime("%Y-%m-%d %H:%M:%S")
    else:
        since = last_date[-1][0]
    return f'journalctl _COMM=sshd -o json --since "{since}" --no-pager'


shell_task = ShellTask(name="Extract", return_all=True)


@task(name="Transform")
def transform(raw_data):
    """
    Takes the raw data returned from the journalctl command and filters / parses it
    down into a database-ready collection of rows.
    """
    data = [json.loads(line) for line in raw_data]
    rows = []

    user_patt = re.compile("user (.*?) from")
    network_patt = re.compile("from (.*?) port (.*?)$")

    db_path = os.path.expanduser("~/GeoLite2-City_20191001/GeoLite2-City.mmdb")
    db_reader = geo_db.Reader(db_path)

    for d in data:
        if user_patt.findall(d["MESSAGE"]) and "Invalid" in d["MESSAGE"]:
            row = {}

            row["timestamp"] = datetime.fromtimestamp(
                int(d["__REALTIME_TIMESTAMP"]) / 1e6
            ).strftime("%Y-%m-%d %H:%M:%S")
            row["username"] = user_patt.findall(d["MESSAGE"])[0]

            ip, port = network_patt.findall(d["MESSAGE"])[0]
            location = db_reader.city(ip)
            row["port"] = int(port)
            row["city"] = location.city.name
            row["country"] = location.country.name
            row["latitude"] = location.location.latitude
            row["longitude"] = location.location.longitude
            rows.append(row)

    return rows


@task
def insert_script(rows):
    """
    Given the cleaned data, creates the SQL Query to insert the new data into the DB.
    """
    insert_cmd = "INSERT INTO SSHATTEMPTS (timestamp, username, port, city, country, latitude, longitude) VALUES\n"
    if not rows:
        raise SKIP("No rows to insert into database.")
    values = (
        ",\n".join(
            [
                "('{timestamp}', '{username}', {port}, '{city}', '{country}', {latitude}, {longitude})".format(
                    **row
                )
                for row in rows
            ]
        )
        + ";"
    )
    return insert_cmd + values


insert = SQLiteScript(name="Insert into DB", db="ssh.db")

## reporting
## - every day, send email report
@task(cache_for=timedelta(hours=24))
def timestamp():
    """
    A cached timestamp task that will be used to determine whether the email report
    should be sent yet.
    """
    return pendulum.now("utc")


class SQLSkip(SQLiteQuery):
    def run(self, timestamp):
        now = pendulum.now("utc")
        if timestamp <= now.add(hours=-1.95):
            raise SKIP("Report sent in the last 24 hours.")
        return super().run()


collect_stats = SQLSkip(
    db="ssh.db",
    query="SELECT username, COUNT(*) as user_count FROM SSHATTEMPTS GROUP BY username ORDER BY user_count DESC LIMIT 50;",
    name="Collect username frequencies",
)


@task
def format_report(stats):
    body = "{x:<15} | {y:>8}".format(x="username", y="count")
    body += "\n" + "-" * len(body) + "\n"
    body += "\n".join(["{x:<15} | {y:>8}".format(x=row[0], y=row[1]) for row in stats])
    return body


email_report = EmailTask(
    name="Email Report",
    subject="Daily SSH username report",
    email_to="chris@prefect.io",
)

schedule = IntervalSchedule(interval=timedelta(hours=2))

with Flow("SSH ETL Monitoring", schedule=schedule) as flow:
    date = last_date(upstream_tasks=[create_table])
    raw_data = shell_task(command=cmd(date))
    clean_data = transform(raw_data)

    db_insert = insert(insert_script(clean_data))

    report_stats = collect_stats(timestamp=timestamp)
    final = email_report(msg=format_report(report_stats))
