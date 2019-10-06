import pendulum

from monitoring_etl_flow import schedule


def test_schedule_runs_every_two_hours():
    next_few = schedule.next(10, after=pendulum.now("utc"))
    diffs = [(y - x).total_hours() for y, x in zip(next_few[1:], next_few)]
    assert set(diffs) == {2.0}


def test_schedule_has_no_end_date():
    assert schedule.end_date is None
