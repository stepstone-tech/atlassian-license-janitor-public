import sys
import json
import requests

def slack_notify(url,title,msg):
    slack_data = {
        "username": "Janitor",
        "icon_emoji": ":satellite:",
        "attachments": [
            {
                "color": "#9733EE",
                "fields": [
                    {
                        "title": title,
                        "value": msg,
                        "short": "false",
                    }
                ]
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(url, data=json.dumps(slack_data), headers=headers,timeout=1000)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
