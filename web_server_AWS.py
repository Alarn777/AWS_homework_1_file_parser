import json
import boto3
import botocore
import requests
import tokens
from boto3.session import Session

aws_access_key_id = "YOUR_ACCESS_KEY_ID"
aws_secret_access_key = "YOUR_SECRET_ACCESS_KEY"

session = Session(aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)
s3 = session.resource('s3')

your_bucket = s3.Bucket('bucket_name')

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
VALID_BUCKET = ""
MAIN_BUCKET = ""
SPAM_BUCKET = ""


def main_loop():
    my_bucket = s3_resource.Bucket('some/path/')

    for obj in my_bucket.objects.all():
        print("Found object" + obj + "\nPossessing...")
        # find object key
        key = my_bucket.get_key(obj)
        file = None  # file to save to
        try:
            s3_resource.Bucket(MAIN_BUCKET).download_file(key, file)
            process_file(json.load(file), key)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    print("Done with the server run")


def send_file_to_spam(file_name):
    first_bucket_name = MAIN_BUCKET
    second_bucket_name = SPAM_BUCKET
    corrupted_file_detected(None)
    try:
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


def process_file(file, KEY):
    file_keys = file.keys()

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
        return

    for key in must_have_keys:
        if key == "courier_ID" and not file[key].is_integer():
            send_file_to_spam(KEY)
        if key == "nuKvar_item_ID" and not file[key].is_integer():
            send_file_to_spam(KEY)

    send_file_to_valid(KEY)


def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    copy_source = {
        'Bucket': bucket_from_name,
        'Key': file_name
    }
    s3_resource.Object(bucket_to_name, file_name).copy(copy_source)
    s3_resource.Object(MAIN_BUCKET, file_name).delete()


def corrupted_file_detected(file):
    url = tokens.url_token
    payload = {'text': 'Corrupted file detected in group-12'}
    # POST with form-encoded data
    print("Corrupted file detected in group-12")
    r = requests.post(url, data=payload)



main_loop()



