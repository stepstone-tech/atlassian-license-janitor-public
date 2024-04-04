"Read README.md in root of the repo for details"
import json
import logging
import requests
from database_ops_with_proxy import DatabaseOperations
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

# Credentials and 

jira_db_credentials = json.load(open("<path to jira DB credentials json file>", 'r', encoding='utf-8'))
bitbucket_db_credentials = json.load(open("<path to bitbucket DB credentials json file>", 'r', encoding='utf-8'))
confluence_db_credentials = json.load(open("<path to confluence DB credentials json file>", 'r', encoding='utf-8'))
slack_hook_url = "<slack webook url>"

jira_token = open("<path to jira HTTP token txt file>", 'r', encoding='utf-8')
jira_api = Jira(
    url='<url.of.your.jira.instance>',
    token=jira_token
)
event = {
    "validateOnly" : True,
    "NUMBER_DAYS_JIRA" : 20,
    "NUMBER_DAYS_JSM" : 10,
    "NUMBER_DAYS_CONFLUENCE" : 20,
    "NUMBER_DAYS_BITBUCKET" : 20,
}
# DAY settings
number_days_jira = event.get("NUMBER_DAYS_JIRA")
number_days_jsm = event.get("NUMBER_DAYS_JSM")
number_days_confluence = event.get("NUMBER_DAYS_CONFLUENCE")
number_days_bitbucket = event.get("NUMBER_DAYS_BITBUCKET")

# collect data section:
# jira

logger_jira = logger.getChild('jira')
database_jira = DatabaseOperations(jira_db_credentials, logger_jira,5433)
jira_inactive_prep = sqlvars.jira_inactive_users_query % number_days_jira
jsm_inactive_prep = sqlvars.jsm_inactive_users_query % number_days_jsm
jira_inactive_users = database_jira.select(jira_inactive_prep)
jsm_inactive_users = database_jira.select(jsm_inactive_prep)

# confluence

logger_confluence = logger.getChild('confluence')
database_confluence = DatabaseOperations(
    confluence_db_credentials, logger_confluence,5434)
confluence_inactive_prep = sqlvars.confluence_inactive_users_query % number_days_confluence
confluence_inactive_users = database_confluence.select(
    confluence_inactive_prep)

# stash

logger_stash = logger.getChild('stash')
database_stash = DatabaseOperations(
    bitbucket_db_credentials, logger_stash,5435)
stash_inactive_prep = sqlvars.stash_inactive_users_query % (
    number_days_bitbucket, number_days_bitbucket, number_days_bitbucket, number_days_bitbucket)
stash_inactive_prep2 = database_stash.select(stash_inactive_prep)
stash_inactive_users = []

#filtering for stash users to be only considered, if they did not receive a license witin last 14 days

for user in stash_inactive_prep2:
    if( database_jira.select(sqlvars.jira_audit_added_to_group_within_14_days % (user[0],user[3]))[0][0] == 0 ):
        stash_inactive_users.append(user)

# validate only section, meaning script will only show number and list of inactive users
        
if (event.get("validateOnly") == True):
    jira_out_users = []
    for user in jira_inactive_users:
        user_name = user[1]
        license_group = user[3]
        jira_out_users.append("user_name"+","+license_group)
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
    
# normal run section able to both list inactive users and remove them from their appropriate license groups
    
if (event.get("validateOnly") == False):

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
    if (len(jira_out_failed_users) == 0):
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
    if (len(jsm_out_failed_users) == 0):
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
    if (len(confluence_out_failed_users) == 0):
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
    if (len(stash_out_failed_users) == 0):
        if len(stash_inactive_users) != 0:
            logger_jira.info(
                "All users removed from stash license groups succesfully")
    else:
        logger_jira.info(
            f"""Failed to remove licences, count:{len(stash_out_failed_users)} , list:\n"""+'\n'.join(stash_out_failed_users))
        failures_detected = True

    jira_current_licensed_users = database_jira.select(
        sqlvars.jira_current_used_licences_query)
    jsm_current_licensed_users = database_jira.select(
        sqlvars.jsm_current_used_licences_query)
    confluence_current_licensed_users = database_confluence.select(
        sqlvars.confluence_current_used_licences_query)
    stash_current_licensed_users = database_stash.select(
        sqlvars.stash_current_used_licences_query)

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


