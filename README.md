# Janitor

## Python Module Dependencies

pg8000,atlassian-python-api,requests

Please be aware that Bitbucket (new name) and Stash (the old name for Bitbucket) are used interchangeably in this repository

General Summary

This Python script is meant to be used to find which users, who've been granted a license to Atlassian tools and are not actively using said license. With the output of the script you can speed up license management. The number of days which determines if a user is viewed as inactive is controlled with a variable in the script number_days_x (where x is the tool name).
Please be aware that this script only has API connection implemented to the Jira Rest API, because in our setup, Jira is used as the main user directory for all other atlassian tools.

Python Summary

Because this script is meant to also be ran as an AWS Lambda, the variables such as number_days_x is stored as a set variable in the code. Additionally, the two main functions of the script are: 
- variable validateOnly is set to True which means the script will not connect to the Jira api to manage licences, it will only output the inactive users and which license group they are using.
- variable validateOnly is set to False which will connect to the Jira API and will remove inactive users from the corresponding license group and follow it up by sending a summary to a Slack Webhook URL as defined in the slack_hook_url variable.


SQL/Database connection summary

The script contains a database_ops.py file with a wrapper class to speed up database querrying which requires dependancies: -pg8000 and -sshtunnel which can be installed with pip or any other suitable method.
The SQLs used for searching in each tool are stored as variables in the sqlvars.py file allowing for easier modification, for each main query (jira,jsm,confluence,bitbucket) there is a corresponding exclusion group that allows for certain users (like administrtors or VIPs) to be excluded from the inactivation mechanism, the groups are as follows script_users_jira,scripts_users_jsm,script_users_confluence,script_users_bitbucket.
Two database_ops files are provided, one containing code for a proxy connection via sshtunnel, the other allowing for a direct connection to the database, use whichever needed, as required.
The database_ops module requires credentials for which a template is available in the INFO_MISC directory

