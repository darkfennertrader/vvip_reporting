import os
import json
import asyncio
from pprint import pprint
import pandas as pd
import boto3
import aiohttp
from aiohttp import ClientError
import pandas as pd
from faker import Faker

from dotenv import load_dotenv


load_dotenv()


# brevo_api = os.getenv("brevo_api_key")
# client_id = os.getenv("uat_client_id")
# username = os.getenv("username")
# password = os.getenv("password")
# URL = "https://uat.virtualvip.club/api"
# PROMO_URL = URL + "/payment/promo"


# def generate_random_email():
#     dummy = Faker()
#     return dummy.email()


# emails_list = []
# for _ in range(2000):
#     emails_list.append(generate_random_email())

# # print(emails_list)


# def _authenticate() -> str:
#     print("\nVirtualVIP Backoffice update PROMO")
#     print("*" * 40)
#     client = boto3.client("cognito-idp", region_name="eu-west-1")

#     # Initiating the Authentication,
#     response = client.initiate_auth(
#         ClientId=client_id,
#         AuthFlow="USER_PASSWORD_AUTH",
#         AuthParameters={
#             "USERNAME": username,
#             "PASSWORD": password,
#         },
#     )

#     return response["AuthenticationResult"]["IdToken"]


# id_token = _authenticate()

# headers = {"Authorization": f"Bearer {id_token}"}


# async def update_contact(session, emails):
#     list_to_update = []
#     for email in emails:
#         json_contact = {
#             "email": email,
#             "type": "business",
#             "promo": "promo6030",
#             "duration": "",
#         }

#         list_to_update.append(json_contact)

#     json_data = {"request": list_to_update}
#     # print()
#     # pprint(json_data, indent=2)

#     try:
#         async with session.put(PROMO_URL, json=json_data, headers=headers) as response:
#             # You might want to check for other status codes here too
#             if response.status == 429:  # Too Many Requests / Rate Limited
#                 print(f"Rate limit hit when updating: {list_to_update}. Retrying...")
#                 await asyncio.sleep(1)  # Wait for a second before retrying
#                 return await update_contact(session, emails)

#             if response.headers.get("Content-Type") == "application/json":
#                 response_data = await response.json()
#             else:
#                 response_data = await response.text()

#             return response_data, response.status

#     except ClientError as e:
#         print(f"Network error occurred: {e}")
#         return {"error": "Network error occurred"}, 500  # Example error response


# async def update_contacts(emails: list[str]):
#     async with aiohttp.ClientSession() as session:
#         # print(type(session))
#         # tasks = [update_contact(session, email) for email in emails]
#         tasks = [update_contact(session, emails)]
#         results = await asyncio.gather(*tasks, return_exceptions=True)
#         return results


# # Run the main coroutine
# result = asyncio.run(update_contacts(emails_list))
# print("\n", result[0])


with open("contacts.json", "r") as file:
    data = json.load(file)


customer_list = []
required_keys = [
    "FIRSTNAME",
    "LASTNAME",
    "Email",
    "TELEFONO",
    "QUALIFICA",
    "ATECO",
    "IMPRESA",
    "FORMAZIONE",
    "TIPO_FORMAZIONE",
    "ANNI_FORMAZIONE_AZIENDA",
    "PENSIERO_SU_AI",
    "LEAD_ORIGIN",
]

# Populate customer_list with dictionaries, ensuring all required keys are present
for elem in data:
    # Make sure the "attributes" dictionary is available
    attributes = elem.get("attributes", {})
    # Copy the "email" to the "Email" field in "attributes"
    attributes["Email"] = elem.get("email", None)
    # Ensure all required fields exist, default to None if they are not present
    customer_dict = {key: attributes.get(key, None) for key in required_keys}
    customer_list.append(customer_dict)

# Create dataframe with all required columns
dataframe = pd.DataFrame(customer_list)
dataframe.sort_index(inplace=True)

# Now dataframe contains all records with specified fields, missing ones are filled with None

print(dataframe.head())
