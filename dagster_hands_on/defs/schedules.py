import dagster as dg


# @dg.schedule(cron_schedule="@daily", target="*")
# def schedules(context: dg.ScheduleEvaluationContext) -> dg.RunRequest | dg.SkipReason:
#     return dg.SkipReason("Skipping. Change this to return a RunRequest to launch a run.")

@dg.schedule(cron_schedule="22 9 * * *", target="*")
def tutorial_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest | dg.SkipReason:
    return dg.RunRequest(run_key=None, run_config={
    })
    # return dg.SkipReason("Skipping. Change this to return a RunRequest to launch a run.")
