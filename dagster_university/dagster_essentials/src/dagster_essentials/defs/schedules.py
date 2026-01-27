
import dagster as dg

from dagster_essentials.defs.jobs import weekly_update_job
from dagster_essentials.defs.jobs import trip_update_job

# Used ScheduleDefinition to create a schedule that: Is attached to the trip_update_job job and have a cron expression
trip_update_schedule = dg.ScheduleDefinition(
    job=trip_update_job,
    cron_schedule="0 0 5 * *", # every 5th of the month at midnight
)

weekly_update_schedule = dg.ScheduleDefinition(
    job=weekly_update_job,   
    cron_schedule="0 0 * * 1", # every Monday at midnight
)

