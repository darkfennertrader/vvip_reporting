import os
import requests
from sib_api_v3_sdk.rest import ApiException
from dotenv import load_dotenv

load_dotenv()


brevo_api = os.getenv("brevo_api_key")


# # create list

# url = "https://api.brevo.com/v3/contacts/lists"

# headers = {
#     "accept": "application/json",
#     "content-type": "application/json",
#     "api-key": brevo_api,
# }
# payload = {"name": "prova", "folderId": 9}
# try:
#     response = requests.post(url, json=payload, headers=headers, timeout=30)
# except ApiException as err:
#     print(f"Exception when calling get_contacts_from_list: {err}\n")


# print(response.text)


# update attribute of multiple contacts

url = "https://api.brevo.com/v3/contacts/batch"
headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "api-key": "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",
}

payload = {
    "contacts": [
        {"attributes": {"Channel": "prova2"}, "email": "virtualvip@formulacoach.it"},
        # {"attributes": {"Channel": "pippo"}, "email": "rai_marino@hotmail.com"},
    ]
}

try:
    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 204:
        print("\n CONTACTS UPDATED\n")
    elif response.status_code == 404:
        print("\nERROR: some email(s) is/are not correct\n")

except ApiException as err:
    print(f"Exception when calling update multiple contacts: {err}\n")
