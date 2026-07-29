"Read README.md in root of the repo for details"
import json
import logging
import os
from datetime import timedelta
import requests
from database_ops import DatabaseOperations
from slack_notif import slack_notify
import sqlvars
from atlassian import Jira

# logging setup

logging.root.setLevel(logging.NOTSET)
logger = logging.getLogger('main')
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)
logger.addHandler(console)


def _require_env(name):
    """Fetches a required environment variable, raising a clear error if it's missing"""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _load_json_file(path):
    """Loads and parses a JSON file"""
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def run_janitor(event):
    """
    Runs one pass of the Atlassian License Janitor.

    event keys:
        validateOnly: bool        -- True = report only, False = remove + notify
        NUMBER_DAYS_JIRA: int
        NUMBER_DAYS_JSM: int
        NUMBER_DAYS_CONFLUENCE: int
        NUMBER_DAYS_BITBUCKET: int

    Returns a summary dict describing what was found/done.
    """

    # Credentials and configuration, sourced from environment variables

    jira_db_credentials_path = _require_env("JANITOR_JIRA_DB_CREDENTIALS_PATH")
    bitbucket_db_credentials_path = _require_env("JANITOR_BITBUCKET_DB_CREDENTIALS_PATH")
    confluence_db_credentials_path = _require_env("JANITOR_CONFLUENCE_DB_CREDENTIALS_PATH")
    jira_token_path = _require_env("JANITOR_JIRA_TOKEN_PATH")
    jira_url = _require_env("JANITOR_JIRA_URL")
    slack_hook_url = _require_env("JANITOR_SLACK_WEBHOOK_URL")

    jira_db_credentials = _load_json_file(jira_db_credentials_path)
    bitbucket_db_credentials = _load_json_file(bitbucket_db_credentials_path)
    confluence_db_credentials = _load_json_file(confluence_db_credentials_path)

    with open(jira_token_path, 'r', encoding='utf-8') as f:
        jira_token = f.read().strip()
    jira_api = Jira(
        url=jira_url,
        token=jira_token
    )

    # DAY settings
    number_days_jira = event.get("NUMBER_DAYS_JIRA")
    number_days_jsm = event.get("NUMBER_DAYS_JSM")
    number_days_confluence = event.get("NUMBER_DAYS_CONFLUENCE")
    number_days_bitbucket = event.get("NUMBER_DAYS_BITBUCKET")
    validate_only = event.get("validateOnly")

    # Bound directly as INTERVAL parameters (pg8000 maps timedelta -> interval)
    interval_jira = timedelta(days=number_days_jira)
    interval_jsm = timedelta(days=number_days_jsm)
    interval_confluence = timedelta(days=number_days_confluence)
    interval_bitbucket = timedelta(days=number_days_bitbucket)

    logger_jira = logger.getChild('jira')
    logger_confluence = logger.getChild('confluence')
    logger_stash = logger.getChild('stash')

    database_jira = DatabaseOperations(jira_db_credentials, logger_jira, local_port=5433)
    database_confluence = DatabaseOperations(confluence_db_credentials, logger_confluence, local_port=5434)
    database_stash = DatabaseOperations(bitbucket_db_credentials, logger_stash, local_port=5435)

    try:
        # collect data section:
        # jira

        jira_inactive_users = database_jira.select(sqlvars.jira_inactive_users_query, (interval_jira,))
        jsm_inactive_users = database_jira.select(sqlvars.jsm_inactive_users_query, (interval_jsm,))

        # confluence

        confluence_inactive_users = database_confluence.select(
            sqlvars.confluence_inactive_users_query, (interval_confluence,))

        # stash

        stash_inactive_prep2 = database_stash.select(
            sqlvars.stash_inactive_users_query,
            (interval_bitbucket, interval_bitbucket, interval_bitbucket, interval_bitbucket))
        stash_inactive_users = []

        # filtering for stash users to be only considered, if they did not receive a license witin last 14 days

        for user in stash_inactive_prep2:
            audit_count = database_jira.select(
                sqlvars.jira_audit_added_to_group_within_14_days, (user[0], user[3]))
            if audit_count[0][0] == 0:
                stash_inactive_users.append(user)

        # validate only section, meaning script will only show number and list of inactive users

        if validate_only:
            jira_out_users = []
            for user in jira_inactive_users:
                user_name = user[1]
                license_group = user[3]
                jira_out_users.append(user_name+","+license_group)
            logger_jira.info(
                f"""Listing inactive jira users, count:{len(jira_inactive_users)} , list:\n"""+';'.join(jira_out_users))
            jsm_out_users = []
            for user in jsm_inactive_users:
                user_name = user[0]
                license_group = user[2]
                jsm_out_users.append(user_name+","+license_group)
            logger_jira.info(
                f"""Listing inactive jsm users, count:{len(jsm_inactive_users)} , list:\n"""+';'.join(jsm_out_users))
            confluence_out_users = []
            for user in confluence_inactive_users:
                user_name = user[0]
                license_group = 'confluence-users'
                confluence_out_users.append(user_name+","+license_group)
            logger_confluence.info(
                f"""Listing inactive confluence users, count:{len(confluence_inactive_users)} , list:\n"""+';'.join(confluence_out_users))
            stash_out_users = []
            for user in stash_inactive_users:
                user_name = user[0]
                license_group = user[3]
                stash_out_users.append(user_name+","+license_group)
            logger_stash.info(
                f"""Listing inactive stash users, count:{len(stash_inactive_users)} , list:\n"""+';'.join(stash_out_users))

            return {
                "validate_only": True,
                "counts": {
                    "jira": len(jira_inactive_users),
                    "jsm": len(jsm_inactive_users),
                    "confluence": len(confluence_inactive_users),
                    "bitbucket": len(stash_inactive_users),
                },
            }

        # normal run section able to both list inactive users and remove them from their appropriate license groups

        # JIRA

        failures_detected = False
        jira_out_users = []
        jira_out_failed_users = []
        for user in jira_inactive_users:
            user_name = user[1]
            license_group = user[3]
            try:
                jira_api.remove_user_from_group(
                    username=user_name, group_name=license_group)
            except requests.HTTPError as error:
                logger_jira.error(
                    f"""Unable to remove {user_name} from {license_group} error {error}""")
                jira_out_failed_users.append(user_name+","+license_group)
            jira_out_users.append(user_name+","+license_group)
        logger_jira.info(
            f"""Inactive jira users, count:{len(jira_inactive_users)} , list:\n"""+';'.join(jira_out_users))
        if len(jira_out_failed_users) == 0:
            if len(jira_inactive_users) != 0:
                logger_jira.info(
                    "All users removed from jira license groups succesfully")
        else:
            logger_jira.warning(
                f"""Failed to remove licences, count:{len(jira_out_failed_users)} , list:\n"""+'\n'.join(jira_out_failed_users))
            failures_detected = True

        # JSM

        jsm_out_users = []
        jsm_out_failed_users = []
        for user in jsm_inactive_users:
            user_name = user[0]
            license_group = user[2]
            try:
                jira_api.remove_user_from_group(
                    username=user_name, group_name=license_group)
            except requests.HTTPError as error:
                logger_jira.error(
                    f"""Unable to remove {user_name} from {license_group} error {error}""")
                jsm_out_failed_users.append(user_name+","+license_group)
            jsm_out_users.append(user_name+","+license_group)
        logger_jira.info(
            f"""Inactive jsm users, count:{len(jsm_inactive_users)} , list:\n"""+';'.join(jsm_out_users))
        if len(jsm_out_failed_users) == 0:
            if len(jsm_inactive_users) != 0:
                logger_jira.info(
                    "All users removed from jsm license groups succesfully")
        else:
            logger_jira.info(
                f"""Failed to remove licences, count:{len(jsm_out_failed_users)} , list:\n"""+'\n'.join(jsm_out_failed_users))
            failures_detected = True

        # CONFLUENCE

        confluence_out_users = []
        confluence_out_failed_users = []
        for user in confluence_inactive_users:
            user_name = user[0]
            license_group = 'confluence-users'
            try:
                jira_api.remove_user_from_group(
                    username=user_name, group_name=license_group)
            except requests.HTTPError as error:
                logger_jira.error(
                    f"""Unable to remove {user_name} from {license_group} error {error}""")
                confluence_out_failed_users.append(user_name+","+license_group)
            confluence_out_users.append(user_name+","+license_group)
        logger_jira.info(
            f"""Inactive confluence users, count:{len(confluence_inactive_users)} , list:\n"""+';'.join(confluence_out_users))
        if len(confluence_out_failed_users) == 0:
            if len(confluence_inactive_users) != 0:
                logger_jira.info(
                    "All users removed from confluence license groups succesfully")
        else:
            logger_jira.info(
                f"""Failed to remove licences, count:{len(confluence_out_failed_users)} , list:\n"""+'\n'.join(confluence_out_failed_users))
            failures_detected = True

        # STASH

        stash_out_users = []
        stash_out_failed_users = []
        for user in stash_inactive_users:
            user_name = user[0]
            license_group = user[3]
            try:
                jira_api.remove_user_from_group(
                    username=user_name, group_name=license_group)
            except requests.HTTPError as error:
                logger_jira.error(
                    f"""Unable to remove {user_name} from {license_group} error {error}""")
                stash_out_failed_users.append(user_name+","+license_group)
            stash_out_users.append(user_name+","+license_group)
        logger_jira.info(
            f"""Inactive stash users, count:{len(stash_inactive_users)} , list:\n"""+';'.join(stash_out_users))
        if len(stash_out_failed_users) == 0:
            if len(stash_inactive_users) != 0:
                logger_jira.info(
                    "All users removed from stash license groups succesfully")
        else:
            logger_jira.info(
                f"""Failed to remove licences, count:{len(stash_out_failed_users)} , list:\n"""+'\n'.join(stash_out_failed_users))
            failures_detected = True

        jira_current_licensed_users = database_jira.select(sqlvars.jira_current_used_licences_query)
        jsm_current_licensed_users = database_jira.select(sqlvars.jsm_current_used_licences_query)
        confluence_current_licensed_users = database_confluence.select(sqlvars.confluence_current_used_licences_query)
        stash_current_licensed_users = database_stash.select(sqlvars.stash_current_used_licences_query)

        try:
            if failures_detected is False:
                slack_notify(slack_hook_url,
                             "The Janitor Lambda finished running without errors",
                             f"""
Licences removed: | Current count:
jira {len(jira_out_users)}, {jira_current_licensed_users[0][0]}
jsm {len(jsm_out_users)}, {jsm_current_licensed_users[0][0]}
confluence {len(confluence_out_users)}, {confluence_current_licensed_users[0][0]}
bitbucket {len(stash_out_users)}, {stash_current_licensed_users[0][0]}
""")
            else:
                logger.error(
                    "One or more failed license removals, please analize above logs.")
                slack_notify(slack_hook_url,
                             "The Janitor Lambda finished running with errors",
                             f"""Some of the licences were not removed
    Licences to be removed: | Current count:
    jira {len(jira_out_users)} of which {len(jira_out_failed_users)} failed, {jira_current_licensed_users[0][0]}
    jsm {len(jsm_out_users)} of which {jsm_out_failed_users} failed, {jsm_current_licensed_users[0][0]}
    confluence {len(confluence_out_users)} of which {len(confluence_out_failed_users)} failed, {confluence_current_licensed_users[0][0]}
    bitbucket {len(stash_out_users)} of which {stash_out_failed_users} failed, {stash_current_licensed_users[0][0]}
    """)
        except Exception:
            # A Slack notification failure shouldn't turn an otherwise-completed
            # (and potentially destructive) run into a failed Lambda invocation,
            # which would trigger an automatic retry of the removals above.
            logger.exception("Failed to send Slack notification")

        return {
            "validate_only": False,
            "failures_detected": failures_detected,
            "removed_counts": {
                "jira": len(jira_out_users),
                "jsm": len(jsm_out_users),
                "confluence": len(confluence_out_users),
                "bitbucket": len(stash_out_users),
            },
        }
    finally:
        database_jira.close()
        database_confluence.close()
        database_stash.close()


if __name__ == "__main__":
    default_event = {
        "validateOnly": True,
        "NUMBER_DAYS_JIRA": 20,
        "NUMBER_DAYS_JSM": 10,
        "NUMBER_DAYS_CONFLUENCE": 20,
        "NUMBER_DAYS_BITBUCKET": 20,
    }
    run_janitor(default_event)
