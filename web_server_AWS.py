#!/usr/bin/env python3
import json
import boto3
import botocore
import requests
from boto3.session import Session

s3_resource = boto3.resource('s3')
VALID_BUCKET = "g12-valid"
MAIN_BUCKET = "g12-raw"
SPAM_BUCKET = "g12-spam"


def main_loop():
    my_bucket = s3_resource.Bucket(MAIN_BUCKET)

    for obj in my_bucket.objects.all():
        # print("Found object" + obj.get()['Body'].read().decode('utf-8') + "\nPossessing...")
        key = obj.key
        my_file = {}
        my_file = obj.get()['Body'].read().decode('utf-8')
        json_content = json.loads(my_file)
        # print(type(json_content))
        process_file(json_content, str(key))

    print("Done with the server run")


def send_file_to_spam(file_name):
    first_bucket_name = MAIN_BUCKET
    second_bucket_name = SPAM_BUCKET
    corrupted_file_detected(file_name)
    try:
        pass
        copy_to_bucket(first_bucket_name, second_bucket_name, file_name)
    except:
        print("ERROR")


def send_file_to_valid(file_name):
    first_bucket_name = MAIN_BUCKET
    second_bucket_name = VALID_BUCKET
    try:
        copy_to_bucket(first_bucket_name, second_bucket_name, file_name)
    except:
        print("ERROR")


def process_file(file_my, KEY):
    file_keys = file_my.keys()
    print(file_my)
    must_have_keys = {"_id",
                      "time",
                      "Sender",
                      "Receiver",
                      "courier_ID",
                      "transaction_date",
                      "item_description",
                      "nuKvar_item_ID"}

    if must_have_keys - file_keys:
        send_file_to_spam(KEY)
        print("Not Valid")
        return

    for key in must_have_keys:
        if key == "courier_ID" and not type(file_my[key]) == int:
            print("Not Valid")
            send_file_to_spam(KEY)
            return

        if key == "nuKvar_item_ID" and not type(file_my[key]) == int:
            send_file_to_spam(KEY)
            print("Not Valid")
            return

    send_file_to_valid(KEY)
    print("Valid " + KEY)


def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    copy_source = {
        'Bucket': bucket_from_name,
        'Key': file_name
    }
    s3_resource.Object(bucket_to_name, file_name).copy(copy_source)
    s3_resource.Object(MAIN_BUCKET, file_name).delete()


def corrupted_file_detected(file_name):
    url = 'https://hooks.slack.com/services/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'
    payload = {
        "text": "corrupted_file_detected : " + file_name,
        "channel": "cloud-2019-group-12"
    }
    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    print(response.status_code)


main_loop()