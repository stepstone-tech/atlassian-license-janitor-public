# Janitor

## Python Module Dependencies

pg8000, atlassian-python-api, requests, sshtunnel (see `infrastructure/apps/janitor/requirements.txt` for pinned versions)

Please be aware that Bitbucket (new name) and Stash (the old name for Bitbucket) are used interchangeably in this repository

General Summary

This Python script is meant to be used to find which users, who've been granted a license to Atlassian tools and are not actively using said license. With the output of the script you can speed up license management. The number of days which determines if a user is viewed as inactive is passed in per-invocation via the `event` dict (`NUMBER_DAYS_JIRA`, `NUMBER_DAYS_JSM`, `NUMBER_DAYS_CONFLUENCE`, `NUMBER_DAYS_BITBUCKET`).
Please be aware that this script only has API connection implemented to the Jira Rest API, because in our setup, Jira is used as the main user directory for all other atlassian tools.

Python Summary

The script's logic lives in `main.py` as a callable `run_janitor(event)` function, so it can be invoked either as an AWS Lambda (via `lambda_handler.py`, which just delegates to `run_janitor`) or locally (`python main.py`, which runs it with a default `event`). The two main behaviors, controlled by the `event["validateOnly"]` flag, are:
- `validateOnly = True`: the script will not connect to the Jira API to manage licences, it will only output the inactive users and which license group they are using.
- `validateOnly = False`: the script will connect to the Jira API and remove inactive users from the corresponding license group, then send a summary to a Slack Webhook URL.

Configuration (credential file paths, the Jira base URL, and the Slack webhook URL) is sourced from environment variables rather than hardcoded in source:

- `JANITOR_JIRA_DB_CREDENTIALS_PATH`
- `JANITOR_BITBUCKET_DB_CREDENTIALS_PATH`
- `JANITOR_CONFLUENCE_DB_CREDENTIALS_PATH`
- `JANITOR_JIRA_TOKEN_PATH`
- `JANITOR_JIRA_URL`
- `JANITOR_SLACK_WEBHOOK_URL`

All six are required; `run_janitor` fails fast with a clear error naming any that's missing.

SQL/Database connection summary

The script contains a `database_ops.py` file with a single `DatabaseOperations` wrapper class to speed up database querying, which requires the `pg8000` and `sshtunnel` dependencies. The class connects through an SSH tunnel when the supplied credentials include a non-empty `SSH_PROXY_HOST`, and connects directly to the database otherwise — so the same class serves both connection modes.
The SQLs used for searching in each tool are stored as variables in the sqlvars.py file allowing for easier modification, for each main query (jira,jsm,confluence,bitbucket) there is a corresponding exclusion group that allows for certain users (like administrtors or VIPs) to be excluded from the inactivation mechanism, the groups are as follows script_users_jira,scripts_users_jsm,script_users_confluence,script_users_bitbucket.
The database_ops module requires credentials for which a template is available in the INFO_MISC directory

