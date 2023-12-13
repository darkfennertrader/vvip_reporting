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
CLIENT_ID = "1rmv00sd7o2qj1deda80gi4v15"
URL = "https://byu1ehuf2i.execute-api.eu-west-1.amazonaws.com/sdlc/api/user-stats-email?mailList="


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


# string to add to the GET
# "mailList=darkfenner69@gmail.com,virtual.vip21333334@yopmail.com"
EMAILS = "vvipuser41+microtest@gmail.com,vvipuser41+testername@gmail.com,vvipuser41+test_pay_1@gmail.com"
EMAILS = "pe3.job@gmail.com"
url = URL + EMAILS


def get_users_stats():
    id_token = _authenticate(CLIENT_ID)
    resp = requests.get(
        url=url,
        headers={
            "Authorization": f"Bearer {id_token}",
        },
        timeout=30,
    )

    print(resp.json())


if __name__ == "__main__":
    get_users_stats()
