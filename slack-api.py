import os
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
    payload = {'text': 'Corrupted file detected in group-12'}
    # POST with form-encoded data
    r = requests.post(url, data=payload)


corrupted_file_detected(None)
