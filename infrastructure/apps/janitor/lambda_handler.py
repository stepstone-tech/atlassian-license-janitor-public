from main import run_janitor


def lambda_handler(event, context):
    """AWS Lambda entrypoint. `event` is expected to carry validateOnly and
    NUMBER_DAYS_JIRA/JSM/CONFLUENCE/BITBUCKET keys (see main.run_janitor)."""
    return run_janitor(event)
