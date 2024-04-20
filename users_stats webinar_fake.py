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

#####################################################


def generate_user_data(num_users):
    # Mean and standard deviation for Gaussian distribution
    mean = 450
    std_dev = 200

    # Generate training points for each user using Gaussian distribution
    np.random.seed(39)
    training_points = np.random.normal(mean, std_dev, num_users)

    # Clip training points to be between 0 and 1000
    training_points = np.clip(training_points, 0, 1000).astype(int)

    # Define correct answers based on the mapping of clipped training points from 0 to 1000 to 0 to 5
    correct_answers_fractional = np.interp(training_points, [0, 1000], [0, 5])

    # Initialize the dictionary
    json_dict = {}

    # Populate the dictionary with user data
    for i in range(1, num_users + 1):
        key = f"user{i}"
        json_dict[key] = {
            "user": {"Email": f"webinar{i}@mail.com"},
            "stats": {
                "trainingPoints": training_points[i - 1],
                "correctAnswers": correct_answers_fractional[i - 1],
            },
        }

    # Add a special message to the dictionary
    json_dict["message"] = (
        "This is a metadata or a special message and should not be accounted in the plot."
    )

    return json_dict


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
            points = user_info["stats"].get("trainingPoints", 0)
            training_points_data.append((email, points))

    df = pd.DataFrame(training_points_data, columns=["Email", "Training Points"])
    df.set_index("Email", inplace=True)
    # df.sort_index(inplace=True)

    # Sort the DataFrame by 'Training Points' column in descending order
    df.sort_values(by="Training Points", ascending=False, inplace=True)

    # Remove duplicate indexes while keeping the first occurrence
    # df = df[~df.index.duplicated(keep="first")]

    return df


def plot_training_points_histogram(data):
    sns.set_context("poster")
    fig, ax = plt.subplots(figsize=(36, 18))

    training_points = []
    correct_answers = []
    zero_points_count = 0
    for key in data.keys():
        if key != "message":
            points = data[key].get("stats", {}).get("trainingPoints", 0)
            answers = data[key].get("stats", {}).get("correctAnswers", 0)
            training_points.append(points)
            correct_answers.append(answers)
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
        bin_answers = [
            answers
            for j, answers in enumerate(correct_answers)
            if digitized[j] - 1 == i
        ]

        absolute_number = f"Users: {len(bin_points)}"
        average_point = (
            f"Avg Points: {np.mean(bin_points):.0f}" if bin_points else "Avg Points: 0"
        )
        average_correct_answers = (
            f"Avg Correct Answers: {np.mean(bin_answers):.2f}"
            if bin_answers
            else "Avg Correct Answers: 0"
        )

        annotation = (
            f"{percent}\n{absolute_number}\n{average_point}\n{average_correct_answers}"
        )
        ax.annotate(
            annotation,
            (p.get_x() + p.get_width() / 2, height),
            xytext=(0, 13),
            textcoords="offset points",
            ha="center",
            fontsize=24,
            color="black",
            bbox=dict(boxstyle="round", fc="#d3e1f1", ec="none"),
        )

    ax.set_title("Training Points Distribution", fontsize=36, fontweight="bold")
    ax.set_xlabel("Points per Session", fontsize=28)
    ax.set_ylabel("Percentage", fontsize=28)
    ax.set_ylim(0, max_height + 10)

    ax.annotate(
        f"Total Users: {total_users}\nNull Sessions: {zero_points_percent:.0f}%",
        xy=(0.80, 0.85),
        fontsize=32,
        xycoords="axes fraction",
        bbox=dict(boxstyle="round", fc="white", ec="none"),
    )

    plt.savefig("webinar_04_22_2024.jpeg")

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
    ATTENDANCE = 75
    start_time = time()
    fake_data = generate_user_data(ATTENDANCE)
    pprint(fake_data)

    ranking = create_training_points_dataframe(fake_data)
    print()
    print(ranking)
    print(len(ranking))
    print(f"The process took {time() - start_time:.3f} sec.")
    plot_training_points_histogram(fake_data)
