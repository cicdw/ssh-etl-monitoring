import datetime
import geoip2.database as geo_db
import json
import pendulum
import re
import sqlite3

from prefect import task, Flow, Parameter
from prefect.tasks.database.sqlite import SQLiteScript, SQLiteQuery
from prefect.tasks.shell import ShellTask

## preliminary setup
## - create database
## - determine last seen date
create_sript = "CREATE TABLE SSHATTEMPTS IF NOT EXISTS (date TEXT PRIMARY KEY, username TEXT, city TEXT, country TEXT, latitude REAL, longitude REAL)"
create_table = SQLiteScript(
    db="ssh.db", script=create_script, name="Create Database and Table"
)
last_date = SQLiteQuery(
    db="ssh.db",
    query="SELECT date FROM SSHATTEMPTS ORDER BY date DESC Limit 1",
    name="Last Seen Date",
)

## data manipulation
## - extract logs since last collected
## - format / extract relevant data fields
## - retrieve location information
## - place into database
@task(name="Format Command")
def cmd(last_date):
    if not last_date:
        since = pendulum.now("utc").add(hours=-48).strftime("%Y-%m-%d %H:%M:%S")
    else:
        since = last_date[-1]
    return f"journalctl _COMM=sshd -o json --since {since} --no-pager"


shell_task = ShellTask(name="Extract")


@task(name="Transform")
def transform(raw_data):
    data = [json.loads(line) for line in raw_data]
    rows = []

    base_pattern = re.compile(".* Invalid user .* from .*")
    user_patt = re.compile("user (.*?) from")
    ip_patt = re.compile("from (.*?)$")

    db_reader = geo_db.Reader("~/GeoLite2-City_20191001/GeoLite2-City.mmdb")

    for d in data:
        if base_pattern.findall(d["MESSAGE"]):
            row = {}

            row["date"] = datetime.fromtimestamp(
                int(d["__REALTIME_TIMESTAMP"]) / 1e6
            ).strftime("%Y-%m-%d %H:%M:%S")
            row["username"] = user_patt.findall(d["MESSAGE"])[0]

            location = db_reader.city(info["ip"])
            row["city"] = location.city.name
            row["country"] = location.country.name
            row["latitude"] = location.location.latitude
            row["longitude"] = location.location.longitude
            rows.append(row)

    return rows


## reporting
## - every day, send email report

with Flow("SSH ETL Monitoring") as flow:
    date = last_date(upstream_tasks=[create_table])
    raw_data = shell_task(command=cmd(date))
    clean_data = transform(raw_data)