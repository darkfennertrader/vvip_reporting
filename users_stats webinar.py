import os
import json
from time import time
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
import seaborn as sns
import matplotlib.pyplot as plt


USERNAME = "darkfenner69@gmail.com"
PASSWORD = "Pippo@45423"

###############    DEV    ###################################
CLIENT_ID = "1rmv00sd7o2qj1deda80gi4v15"
URL = "https://byu1ehuf2i.execute-api.eu-west-1.amazonaws.com/sdlc/api/user-stats-email"

################   UAT   ###########################

CLIENT_ID = "4ekkk4f3u9uae70midfdqse4k6"
URL = "https://49trqc7yl3.execute-api.eu-west-1.amazonaws.com/uat/api/user-stats-email"


################   PROD   ###########################

# CLIENT_ID = "5j2ud20g3tv340ugdejnhuv42o"
# URL = "https://oaxqfw4wb7.execute-api.eu-west-1.amazonaws.com/prod/api/user-stats-email"


def deduplicate_data(data):
    """_summary_ eliminate duplicated user sessions (now fixed bug)"""
    new_data = {}
    seen_emails = set()

    for key, value in data.items():
        if key != "message":
            user_info = value
            email = user_info["user"]["Email"]
            if email not in seen_emails:
                seen_emails.add(email)
                new_data[key] = value

    # If needed, add back the 'message' key if it's in the original data
    if "message" in data:
        new_data["message"] = data["message"]

    return new_data


def create_training_points_dataframe(data):
    training_points_data = []
    for key in data.keys():
        if key != "message":
            user_info = data[key]
            email = user_info["user"]["Email"]
            points = user_info["stats"].get("lastSessionPoints", 0)
            training_points_data.append((email, points))

    df = pd.DataFrame(training_points_data, columns=["Email", "Last Session Points"])
    df.set_index("Email", inplace=True)
    # df.sort_index(inplace=True)

    # Sort the DataFrame by 'Training Points' column in descending order
    df.sort_values(by="Last Session Points", ascending=False, inplace=True)

    # Remove duplicate indexes while keeping the first occurrence
    # df = df[~df.index.duplicated(keep="first")]

    return df


def plot_training_points_histogram(data):
    sns.set_context("poster")
    fig, ax = plt.subplots(figsize=(36, 18))

    training_points = []
    zero_points_count = 0
    for key in data.keys():
        if key != "message":
            points = data[key].get("stats", {}).get("trainingPoints", 0)
            training_points.append(points)
            if points == 0:
                zero_points_count += 1

    total_users = len(training_points)
    if total_users == 0:
        print("No users found. Exiting function.")
        return

    zero_points_percent = (zero_points_count / total_users) * 100

    if zero_points_count == total_users:
        print("All training points are zero. Exiting function.")
        return

    min_points, max_points = min(training_points), max(training_points)
    # Adjust the bin definition
    bins = np.linspace(0, max_points, 6)
    if max_points == bins[-1]:
        bins[-1] += 1e-10  # Adjust the last bin to ensure it includes the exact maximum

    hist = sns.histplot(training_points, stat="percent", bins=bins, kde=True, ax=ax)

    # Digitizing the values, the right parameter ensures rightmost edge inclusion
    digitized = np.digitize(training_points, bins, right=True)

    max_height = 0
    for i, p in enumerate(hist.patches):
        height = p.get_height()

        if height > max_height:
            max_height = height  # Update max height

        percent = f"Percentage: {height:.0f}%"
        bin_points = [
            points for j, points in enumerate(training_points) if digitized[j] - 1 == i
        ]
        absolute_number = f"Users: {len(bin_points)}"
        average_point = (
            f"Avg Points: {np.mean(bin_points):.0f}" if bin_points else "Avg Points: 0"
        )

        annotation = f"{percent}\n{absolute_number}\n{average_point}"
        ax.annotate(
            annotation,
            (p.get_x() + p.get_width() / 2, height),
            xytext=(0, 0),
            textcoords="offset points",
            ha="center",
            fontsize=24,
            color="black",
            bbox=dict(boxstyle="round", fc="#d3e1f1", ec="none"),
        )

    ax.set_title("Training Points Distribution", fontsize=36, fontweight="bold")
    ax.set_xlabel("Points per Session", fontsize=28)
    ax.set_ylabel("Percentage", fontsize=28)
    ax.set_ylim(0, max_height + 5)

    ax.annotate(
        f"Total Users: {total_users}\nZero Points: {zero_points_percent:.0f}%",
        xy=(0.80, 0.85),
        fontsize=32,
        xycoords="axes fraction",
        bbox=dict(boxstyle="round", fc="white", ec="none"),
    )

    plt.savefig("trial_stats_prod.jpeg")

    plt.show()
    plt.close()


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


def get_users_stats(email_list):
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


if __name__ == "__main__":

    PARTICIPANTS = 150

    email_list = [f"webinar{number}@mail.com" for number in range(1, PARTICIPANTS + 1)]
    # print(email_list[:5])

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

    start_time = time()
    json_list = get_users_stats(email_list)
    json_list = deduplicate_data(json_list)
    # pprint(json_list)

    ranking = create_training_points_dataframe(json_list)
    print()
    print(ranking)
    print(len(ranking))
    print(f"The process took {time() - start_time:.3f} sec.")
    plot_training_points_histogram(json_list)


# {'0': {'stats': {'badges': {'Activity': 'Grower',
#                             'Atlantic Coin': None,
#                             'Eloquence': None,
#                             'Perfectionism': None,
#                             'Sessions': 'Newbie',
#                             'Top Performance': None},
#                  'chattingTimeHours': 0,
#                  'chattingTimeMinutes': 17,
#                  'chattingTimeSeconds': 14,
#                  'daysSinceLastSession': 0,
#                  'lastUnlockedPath': 'general_be_effective',
#                  'numberOfEarnedBadges': 3,
#                  'numberOfUnlockedPaths': 1,
#                  'sessionsCompleted': 3,
#                  'sessionsDropped': 3,
#                  'trainingPoints': 2368,
#                  'trainingSessionLastDate': '2024-04-18 12:41:02'},
#        'user': {'Email': 'leonardo_1@mail.com',
#                 'Name': 'Test',
#                 'Nickname': 'leonardo_1',
#                 'Surname': 'test',
#                 'Username': '34e82b05-8bef-4890-b70c-2992860963f9'}},
#  '1': {'stats': {'badges': {'Activity': 'Newbie',
#                             'Atlantic Coin': None,
#                             'Eloquence': 'Grower',
#                             'Perfectionism': None,
#                             'Sessions': 'Grower',
#                             'Top Performance': None},
#                  'chattingTimeHours': 0,
#                  'chattingTimeMinutes': 38,
#                  'chattingTimeSeconds': 6,
#                  'daysSinceLastSession': 0,
#                  'lastUnlockedPath': 'be_proactive',
#                  'numberOfEarnedBadges': 5,
#                  'numberOfUnlockedPaths': 2,
#                  'sessionsCompleted': 6,
#                  'sessionsDropped': 0,
#                  'trainingPoints': 3764,
#                  'trainingSessionLastDate': '2024-04-18 12:41:03'},
#        'user': {'Email': 'leonardo_2@mail.com',
#                 'Name': 'Test',
#                 'Nickname': 'leonardo_2',
#                 'Surname': 'test',
#                 'Username': '92ee88d2-8b51-44c2-b7ee-b5fbb3ec0ca6'}},
#  'message': None}
