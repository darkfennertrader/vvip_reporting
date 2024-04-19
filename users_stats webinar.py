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

CLIENT_ID = "5j2ud20g3tv340ugdejnhuv42o"
URL = "https://oaxqfw4wb7.execute-api.eu-west-1.amazonaws.com/prod/api/user-stats-email"


# def plot_training_points_histogram(data):
#     # Adjusting size and context for larger plot
#     sns.set_context("poster")  # Larger fonts

#     fig, ax = plt.subplots(figsize=(36, 18))  # Using subplots with a defined axis 'ax'

#     # Extract training points for each user
#     training_points = []
#     zero_points_count = 0
#     for key in data.keys():
#         if key != "message":  # Skipping non-user data keys
#             points = data[key]["stats"]["trainingPoints"]
#             training_points.append(points)
#             if points == 0:
#                 zero_points_count += 1

#     # Total number of users
#     total_users = len(training_points)

#     # Handle case where there are no users to prevent division by zero
#     if total_users == 0:
#         zero_points_percent = 0
#     else:
#         # Percentage with trainingPoints = 0
#         zero_points_percent = (zero_points_count / total_users) * 100

#     # Prevent further execution if no training points data is available
#     if total_users == 0:
#         print("No users found. Exiting function.")
#         return

#     # Define dynamic bins based on the range of training points, prevent plotting if no range
#     if training_points:
#         min_points = min(training_points)
#         max_points = max(training_points)
#         bins = np.linspace(min_points, max_points, 11)
#     else:
#         print("No valid training points available.")
#         return

#     # Plotting histogram with KDE and dynamic bins
#     hist = sns.histplot(training_points, stat="percent", bins=bins, kde=True, ax=ax)

#     # Adding percentage text above each bar
#     for p in hist.patches:
#         height = p.get_height()
#         ax.text(
#             p.get_x() + p.get_width() / 2.0,
#             height + 0.1,
#             f"{height:.0f}%",
#             ha="center",
#             va="bottom",
#             fontsize=24,
#             color="black",
#         )

#     # Using set_title and set_ylabel to set title and y-axis label respectively
#     ax.set_title("Training Points Distribution", fontsize=24, fontweight="bold")
#     ax.set_ylabel("Percentage", fontsize=20)

#     # Adding box with user stats using 'annotate'
#     ax.annotate(
#         f"Total Users: {total_users}\nZero Points %: {zero_points_percent:.1f}%",
#         xy=(0.70, 0.85),
#         xycoords="axes fraction",
#         bbox=dict(boxstyle="round", fc="1"),
#     )

#     plt.show()


def deduplicate_data(data):
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
            points = user_info["stats"].get("trainingPoints", 0)
            training_points_data.append((email, points))

    df = pd.DataFrame(training_points_data, columns=["Email", "Training Points"])
    df.set_index("Email", inplace=True)

    # Sort the DataFrame by 'Training Points' column in descending order
    df.sort_values(by="Training Points", ascending=False, inplace=True)

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
        zero_points_percent = 0
    else:
        zero_points_percent = (zero_points_count / total_users) * 100

    if total_users == 0:
        print("No users found. Exiting function.")
        return

    if not training_points:
        print("No valid training points available.")
        return

    min_points, max_points = min(training_points), max(training_points)
    bins = np.linspace(min_points, max_points, 6)

    hist = sns.histplot(training_points, stat="percent", bins=bins, kde=True, ax=ax)

    for p in hist.patches:
        height = p.get_height()
        percent = f"{height:.0f}%"
        absolute_number = f"Abs: {int(height * total_users / 100)}"
        average_point = f"Avg: {p.get_x() + p.get_width()/2:.1f}"

        annotation = f"{percent}\n{absolute_number}\n{average_point}"

        ax.annotate(
            annotation,
            (p.get_x() + p.get_width() / 2, height),  # slightly below the upper edge
            xytext=(0, -10),  # shifting text up
            textcoords="offset points",
            ha="center",
            fontsize=20,
            color="black",
            bbox=dict(boxstyle="round", fc="#d3e1f1", ec="none"),
        )

    ax.set_title("Training Points Distribution", fontsize=24, fontweight="bold")
    ax.set_ylabel("Percentage", fontsize=20)

    ax.annotate(
        f"Total Users: {total_users}\nZero Points %: {zero_points_percent:.1f}%",
        xy=(0.70, 0.85),
        xycoords="axes fraction",
        bbox=dict(boxstyle="round", fc="white", ec="none"),
    )

    plt.show()


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
        "leonardo_1@mail.com",
        "leonardo_2@mail.com",
        "leonardo_3@mail.com",
        "leonardo_4@mail.com",
        "leonardo_5@mail.com",
        "leonardo_6@mail.com",
        "andrea_1@mail.com",
        "andrea_2@mail.com",
        "andrea_3@mail.com",
        "andrea_4@mail.com",
        "andrea_5@mail.com",
        "andrea_6@mail.com",
        "paolo_1@mail.com",
        "paolo_2@mail.com",
        "paolo_3@mail.com",
        "paolo_4@mail.com",
        "paolo_5@mail.com",
        "paolo_6@mail.com",
        "razvan_1@mail.com",
        "razvan_2@mail.com",
        "razvan_3@mail.com",
        "razvan_4@mail.com",
        "razvan_5@mail.com",
        "razvan_6@mail.com",
        "raimondo_1@mail.com",
        "raimondo_2@mail.com",
        "raimondo_3@mail.com",
        "raimondo_4@mail.com",
        "raimondo_5@mail.com",
        "raimondo_6@mail.com",
        "jacopo_1@mail.com",
        "jacopo_2@mail.com",
        "jacopo_3@mail.com",
        "jacopo_4@mail.com",
        "jacopo_5@mail.com",
        "jacopo_6@mail.com",
    ]

    start_time = time()
    json_list = get_users_stats(email_list)

    pprint(json_list)

    json_list = deduplicate_data(json_list)

    print("after deduplication")
    pprint(json_list)

    ranking = create_training_points_dataframe(json_list)
    print()
    print(ranking)
    print(f"The process took {time() - start_time:.3f} sec.")
    plot_training_points_histogram(json_list)
