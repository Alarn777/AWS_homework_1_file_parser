import os
import json
import requests
import tokens
# from slackclient import SlackClient
#
# slack_token = os.environ["SLACK_API_TOKEN"]
# sc = SlackClient(slack_token)
#
#
# def corrupted_file_detected(file):
#     sc.api_call(
#       "chat.postMessage",
#       channel="CHNE3E2MB",
#       text="Corrupted File Detected!"
#     )


def corrupted_file_detected(file):
    url = tokens.url_token
    payload = {
        "text": "Test 3:13",
        "channel": "cloud-2019-group-12"
    }
    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    print(response.status_code)


corrupted_file_detected(None)
