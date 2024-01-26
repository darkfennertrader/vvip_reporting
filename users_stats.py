import os
import json
from typing import Any
from datetime import datetime
import collections
from pprint import pprint
import asyncio
import pandas as pd
import numpy as np
from sib_api_v3_sdk.rest import ApiException
import aiohttp
from aiohttp import ClientError
import requests
import boto3


USERNAME = "darkfenner69@gmail.com"
PASSWORD = "Pippo@45423"

###############    DEV    ###################################
CLIENT_ID = "1rmv00sd7o2qj1deda80gi4v15"
URL = "https://byu1ehuf2i.execute-api.eu-west-1.amazonaws.com/sdlc/api/user-stats-email"

################   UAT   ###########################

CLIENT_ID = "4ekkk4f3u9uae70midfdqse4k6"
URL = "https://49trqc7yl3.execute-api.eu-west-1.amazonaws.com/uat/api/user-stats-email"


def _authenticate(client_id) -> str:
    print("\nVirtualVIP Backoffice data")
    print("*" * 30)
    client = boto3.client("cognito-idp", region_name="eu-west-1")

    # Initiating the Authentication,
    response = client.initiate_auth(
        ClientId=CLIENT_ID,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={
            "USERNAME": USERNAME,
            "PASSWORD": PASSWORD,
        },
    )

    return response["AuthenticationResult"]["IdToken"]
    # return response


def get_users_stats():
    id_token = _authenticate(CLIENT_ID)
    resp = requests.post(
        url=URL,
        headers={
            "Authorization": f"Bearer {id_token}",
        },
        timeout=30,
        json={"mailList": ["darkfenner69@gmail.com"]},
    )

    print(resp.json())


if __name__ == "__main__":
    get_users_stats()
