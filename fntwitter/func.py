import io
import requests
import os
import json
import logging
import sys
import time
from datetime import datetime, timedelta
import configparser

from fdk import response
import oci.object_storage


config = configparser.ConfigParser()
config.read('config.ini')
os.environ['BEARER_TOKEN'] = '<TOKEN>'

def auth():
    return os.environ.get("BEARER_TOKEN")

def create_url():
    now = datetime.now() + timedelta(seconds = -10)
    now_minus_10 = now + timedelta(minutes = -3)
    end_date = now.strftime('%Y-%m-%d')
    end_time = now.strftime('%H:%M:%S')
    start_date = now_minus_10.strftime('%Y-%m-%d')
    start_time = now_minus_10.strftime('%H:%M:%S')
    query = "bbb22"
    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,referenced_tweets,text"
    user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
    filters = "start_time="+start_date+"T"+start_time+".00Z&end_time="+end_date+"T"+end_time+".00Z"
    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}".format(
        query, tweet_fields, user_fields, filters
    )
    return url


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def paginate(url, headers, next_token=""):
    if next_token:
        full_url = f"{url}&next_token={next_token}"
    else:
        full_url = url
    data = connect_to_endpoint(full_url, headers)
    yield data
    if "next_token" in data.get("meta", {}):
        yield from paginate(url, headers, data['meta']['next_token'])

def get_api_twitter():
    #append_api = []
    append_api = ""
    bearer_token = auth()
    url = create_url()
    headers = create_headers(bearer_token)
    for json_response in paginate(url, headers):
        #append_api.append(json.dumps(json_response, indent=4, sort_keys=True))
        append_api += json.dumps(json_response, sort_keys=True)
    #return "".join([str(item) for item in append_api])
    return_api = "["+append_api.replace("}{", "},{")+"]"
    return return_api

def handler(ctx, data: io.BytesIO=None):
    vtime = time.strftime("%Y%m%d-%H%M%S")
    bucketName = config.get('DEFAULT', 'bucketName')
    objectName = "twitter_api" + vtime + ".json"
    content = get_api_twitter()
    resp = put_object(bucketName, objectName, content)
    return response.Response(
        ctx,
        response_data=json.dumps(resp),
        headers={"Content-Type": "application/json"}
    )

def put_object(bucketName, objectName, content):
    signer = oci.auth.signers.get_resource_principals_signer()
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    namespace = client.get_namespace().data
    output=""
    try:
        object = client.put_object(namespace, bucketName, objectName, content)
        output = "Success: Put object '" + objectName + "' in bucket '" + bucketName + "'"
    except Exception as e:
        output = "Failed: " + str(e)
    return { "state": output }

