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


################   PROD   ###########################

CLIENT_ID = "5j2ud20g3tv340ugdejnhuv42o"
URL = "https://oaxqfw4wb7.execute-api.eu-west-1.amazonaws.com/prod/api/user-stats-email"


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


email_list = [
    "leonardo_emp_1@mail.com",
    "leonardo_emp_2@mail.com",
    "leonardo_emp_3@mail.com",
    "leonardo_emp_4@mail.com",
    "leonardo_emp_5@mail.com",
    "leonardo_emp_6@mail.com",
    "andrea_emp_1@mail.com",
    "andrea_emp_2@mail.com",
    "andrea_emp_3@mail.com",
    "andrea_emp_4@mail.com",
    "andrea_emp_5@mail.com",
    "andrea_emp_6@mail.com",
    "paolo_emp_1@mail.com",
    "paolo_emp_2@mail.com",
    "paolo_emp_3@mail.com",
    "paolo_emp_4@mail.com",
    "paolo_emp_5@mail.com",
    "paolo_emp_6@mail.com",
    "razvan_emp_1@mail.com",
    "razvan_emp_2@mail.com",
    "razvan_emp_3@mail.com",
    "razvan_emp_4@mail.com",
    "razvan_emp_5@mail.com",
    "razvan_emp_6@mail.com",
    "raimondo_emp_1@mail.com",
    "raimondo_emp_2@mail.com",
    "raimondo_emp_3@mail.com",
    "raimondo_emp_4@mail.com",
    "raimondo_emp_5@mail.com",
    "raimondo_emp_6@mail.com",
    "jacopo_emp_1@mail.com",
    "jacopo_emp_2@mail.com",
    "jacopo_emp_3@mail.com",
    "jacopo_emp_4@mail.com",
    "jacopo_emp_5@mail.com",
    "jacopo_emp_6@mail.com",
]

email_list = [
    "darkfenner69@gmail.com",
    "giordano@formulacoach.it",
    "darkfenner69+test_aug11_01@gmail.com",
]

# email_list = [
#     "darkfenner69@gmail.com",
#     "giordano@formulacoach.it",
#     "raimondo.marino69@gmail.com",
# ]

# email_list = ["giordano@formulacoach.it", "darkfenner69+test_aug11_01@gmail.com"]


def get_users_stats():
    id_token = _authenticate(CLIENT_ID)
    resp = requests.post(
        url=URL,
        headers={
            "Authorization": f"Bearer {id_token}",
        },
        timeout=30,
        json={"mailList": email_list},
    )

    return resp.json()


def from_dict_to_dataframe(data):
    # Initialize an empty list to store the rows
    rows = []

    # Iterate over the keys in the data dictionary
    for key in data:
        if key == "message":
            continue

        user_info = data[key].get("user", {})
        stats = data[key].get("stats", {})
        badges = stats.get("badges", {})

        # Create a row with the required structure
        row = {
            "Name": user_info.get("Name", None),
            "LastName": user_info.get("Surname", None),
            "Email": user_info.get("Email", None),
            "trainingPoints": stats.get("trainingPoints", None),
            "trainingSessionLastDate": stats.get("trainingSessionLastDate", None),
            "sessionsCompleted": stats.get("sessionsCompleted", None),
            "sessionsDropped": stats.get("sessionsDropped", None),
            "chattingTimeHours": stats.get("chattingTimeHours", None),
            "chattingTimeMinutes": stats.get("chattingTimeMinutes", None),
            "chattingTimeSeconds": stats.get("chattingTimeSeconds", None),
            "numberOfEarnedBadges": stats.get("numberOfEarnedBadges", None),
            "lastUnlockedPath": stats.get("lastUnlockedPath", None),
            "numberOfUnlockedPaths": stats.get("numberOfUnlockedPaths", None),
            "daysSinceLastSession": stats.get("daysSinceLastSession", None),
            "lastSessionPoints": stats.get("lastSessionPoints", None),
            "lastSessionCorrectAnswers": stats.get("lastSessionCorrectAnswers", None),
            "totalCorrectAnswers": stats.get("totalCorrectAnswers", None),
            "Eloquence": badges.get("Eloquence", None),
            "Sessions": badges.get("Sessions", None),
            "Activity": badges.get("Activity", None),
            "Top Performance": badges.get("Top Performance", None),
            "Perfectionism": badges.get("Perfectionism", None),
        }

        # Append the row to the list of rows
        rows.append(row)

        df = pd.DataFrame(rows)
        df.sort_values(by="trainingPoints", ascending=False, inplace=True)

    return df


if __name__ == "__main__":
    data = get_users_stats()
    print(data)
    ranking = from_dict_to_dataframe(data)
    print(ranking)
