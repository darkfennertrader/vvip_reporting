import os
import json
import requests
from sib_api_v3_sdk.rest import ApiException
import pandas as pd
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

# url = "https://api.brevo.com/v3/contacts/batch"
# headers = {
#     "accept": "application/json",
#     "content-type": "application/json",
#     "api-key": "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",
# }

# payload = {
#     "contacts": [
#         {"attributes": {"Channel": "prova2"}, "email": "virtualvip@formulacoach.it"},
#         # {"attributes": {"Channel": "pippo"}, "email": "rai_marino@hotmail.com"},
#     ]
# }

# try:
#     response = requests.post(url, json=payload, headers=headers)

#     if response.status_code == 204:
#         print("\n CONTACTS UPDATED\n")
#     elif response.status_code == 404:
#         print("\nERROR: some email(s) is/are not correct\n")

# except ApiException as err:
#     print(f"Exception when calling update multiple contacts: {err}\n")


url_get = "https://api.brevo.com/v3/contacts/lists/4/contacts?limit=500"

dataframe = pd.DataFrame()

try:
    resp = requests.get(
        url=url_get,  # type: ignore
        headers={
            "accept": "application/json",
            "api-key": os.getenv("brevo_api_key"),
        },  # type: ignore
        timeout=30,
    )
    # print(resp.status_code)
    data = json.loads(resp.text)
    print(len(data["contacts"]))

    if isinstance(url_get, str):
        pass
    else:
        raise ValueError("returned none value")

except ApiException as err:
    print(f"Exception when calling get_contacts_from_list: {err}\n")


# customer_list = []
# for elem in data["contacts"]:
#     elem["attributes"]["Email"] = elem["email"]
#     customer_list.append(elem["attributes"])

# # print(pd.DataFrame(customer_list))
# dataframe = pd.DataFrame(customer_list)

# dataframe = dataframe[~dataframe["Email"].str.contains("formulacoach.it")]
# dataframe = dataframe[~dataframe["Email"].str.contains("neting.it")]

# # selecting a subset of columns:
# dataframe = dataframe[
#     [
#         "FIRSTNAME",
#         "LASTNAME",
#         "Email",
#         "TELEFONO",
#         "QUALIFICA",
#         "ATECO",
#         "IMPRESA",
#         "FORMAZIONE",
#         "TIPO_FORMAZIONE",
#         "ANNI_FORMAZIONE_AZIENDA",
#         "PENSIERO_SU_AI",
#     ]
# ]

# # renaming columns
# dataframe.rename(
#     columns={
#         "FIRSTNAME": "Nome",
#         "LASTNAME": "Cognome",
#         "TELEFONO": "Telefono",
#         "QUALIFICA": "Qualifica",
#         "ATECO": "Settore",
#         "IMPRESA": "Impresa",
#         "FORMAZIONE": "Formazione",
#         "TIPO_FORMAZIONE": "Domanda1",
#         "ANNI_FORMAZIONE_AZIENDA": "Domanda2",
#         "PENSIERO_SU_AI": "Domanda3",
#     },
#     inplace=True,
# )

# if list_url == os.getenv("brevo_url"):
#     dataframe["Form"] = "complete"
# elif list_url == os.getenv("brevo_url_missing"):
#     dataframe["Form"] = "missing"

# dataframe["Agency"] = "neting"
# dataframe["RCT_group"] = "Business_Sales"

# # print(dataframe.head())

# dataframe.drop(
#     columns=["CHANNEL", "STATUS"], axis=1, errors="ignore", inplace=True
# )

# dataframe.set_index("Email", inplace=True)
# # print(dataframe.columns)
