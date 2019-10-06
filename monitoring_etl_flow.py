import datetime
import geoip2.database as geo_db
import json
import re
import sqlite3

from prefect import task, Flow, Parameter
from prefect.tasks.database.sqlite import SQLiteScript, SQLiteQuery
from prefect.tasks.shell import ShellTask

## preliminary setup
## - create database
## - determine last seen date
create_sript = "CREATE TABLE SSHATTEMPTS IF NOT EXISTS (date TEXT PRIMARY KEY, username TEXT, city TEXT, country TEXT, latitude REAL, longitude REAL)"
create_task = SQLiteScript(
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
    return "journalctl _COMM=sshd -o json --since {} --no-pager"


shell_task = ShellTask(name="Extract")


@task(name="Transform")
def transform(raw_data):
    data = [json.loads(line) for line in raw_data]
    rows = []
    pattern = re.compile(".* Invalid user .* from .*")
    user_patt = re.compile("user (.*?) from")
    ip_patt = re.compile("from (.*?)$")
    db_reader = geo_db.Reader("~/GeoLite2-City_20191001/GeoLite2-City.mmdb")

    for d in data:
        if pattern.findall(d["MESSAGE"]):
            row = {}
            row["date"] = datetime.fromtimestamp(int(d["__REALTIME_TIMESTAMP"]) / 1e6)
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
