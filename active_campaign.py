import os
import json
from pprint import pprint
import requests
import pandas as pd
from dotenv import load_dotenv
from sib_api_v3_sdk.rest import ApiException


load_dotenv()


# active_url = os.getenv("active_url")
# headers = {"accept": "application/json", "Api-Token": os.getenv("active_api")}

# ## build dict with custom fields
# url = str(active_url) + "fields"
# response = requests.get(url, headers=headers, timeout=30)
# resp = json.loads(response.text)
# # pprint(resp["fields"], indent=2)

# CUSTOM_FIELDS = {}
# for field in resp["fields"]:
#     # print(f"{field['id']}: {field['perstag']}")
#     CUSTOM_FIELDS[field["id"]] = field["perstag"]
#     # print(type(field["id"]))
#     # print(type(field["perstag"]))

# pprint(CUSTOM_FIELDS, indent=2)

# with open("./data/itera_custom_fields.json", "w") as file:
#     json.dump(CUSTOM_FIELDS, file)

# url = "https://virtualvip.api-us1.com/api/3/contacts?listid=1"


# response = requests.get(url, headers=headers, timeout=30)
# resp = json.loads(response.text)

# print("\nresp[contacts]:")
# # pprint(resp["contacts"])

# list_of_contacts = []
# list_of_ids = []
# for contact in resp["contacts"]:
#     print("\ncontact:")
#     print(contact)
#     print("*" * 60)
#     ids_dict = {}
#     contact_dict = {}

#     ids_dict[contact["email"]] = contact["id"]

#     contact_dict["Name"] = contact["firstName"]
#     contact_dict["Surname"] = contact["lastName"]
#     contact_dict["Email"] = contact["email"]
#     contact_dict["Telefono"] = contact["phone"]

#     url = f"https://virtualvip.api-us1.com/api/3/contacts/{contact['id']}/fieldValues"
#     attributes = requests.get(url, headers=headers, timeout=30)
#     resp = json.loads(attributes.text)
#     pprint(json.loads(attributes.text), indent=2)

#     _dict = {}
#     for contact in resp["fieldValues"]:
#         print("\nprinting contact")
#         pprint(contact, indent=2)
#         # print(f"{CUSTOM_FIELDS[contact['field']]}: {contact['value']}")
#         if contact["field"]:
#             contact_dict[CUSTOM_FIELDS[contact["field"]]] = contact["value"]
#         if contact["field"] in ["8", "9"]:
#             _dict[contact["field"]] = contact["id"]

#     ids_dict.update(_dict)
#     list_of_ids.append(ids_dict)
#     list_of_contacts.append(contact_dict)

#     print("*" * 60)
#     print("*" * 60)


# data = pd.DataFrame.from_records(list_of_contacts)
# print(data)
# print(data.shape)
# with open("./data/itera_contacts.json", "w") as file:
#     json.dump(list_of_ids, file)
################################################################################


# UPDATE CONTACT ATTRIBUTES

# # # load Customer "SALES_CHANNEL"
# itera_customers = pd.read_csv("./data/itera_customers.csv", index_col=["Email"])
# print(itera_customers)

# # update only those Customers that have a NaN value in the "SALES_CHANNEL" column
# to_update = itera_customers[itera_customers["SALES_CHANNEL"].isnull()]
# print(to_update)

# # load custom fields attributes
# with open("./data/itera_custom_fields.json", "r") as file:
#     customer_fields = json.load(file)

# # load Customer id attributes
# with open("./data/itera_contacts.json", "r") as file:
#     customer_contacts = json.load(file)


# CREATE A CUSTOM FIELD VALUE

# url = "https://virtualvip.api-us1.com/api/3/fieldValues"

# payload = {
#     "fieldValue": {"contact": "37", "field": "8", "value": "test2"},
#     "useDefaults": False,
# }
# headers = {
#     "accept": "application/json",
#     "content-type": "application/json",
#     "Api-Token": os.getenv("active_api"),
# }

# response = requests.post(url, json=payload, headers=headers, timeout=30)

# print(response.text)


###### data from Leads with missing data #######################################

headers = {"accept": "application/json", "Api-Token": os.getenv("active_api")}
active_url = "https://virtualvip.api-us1.com/api/3/"

## build dict with custom fields
url = str(active_url) + "fields"
try:
    response = requests.get(url, headers=headers, timeout=30)
    resp = json.loads(response.text)
    custom_fields = {}
    for field in resp["fields"]:
        # print(f"{field['id']}: {field['perstag']}")
        custom_fields[field["id"]] = field["perstag"]

    # save custom fields for later contacts update
    with open("./data/itera_custom_fields.json", "w") as file:
        json.dump(custom_fields, file)

    url = str(active_url) + "contacts?listid=2"
    try:
        response = requests.get(url, headers=headers, timeout=30)
        resp = json.loads(response.text)

        list_of_contacts = []
        list_of_ids = []

        for contact in resp["contacts"]:
            ids_dict = {}
            contact_dict = {}
            url = str(active_url) + f"contacts/{contact['id']}/fieldValues"
            attributes = requests.get(url, headers=headers, timeout=30)

            ids_dict[contact["email"]] = contact["id"]

            # pprint(json.loads(attributes.text), indent=2)
            contact_dict["Nome"] = contact["firstName"]
            contact_dict["Cognome"] = contact["lastName"]
            contact_dict["Email"] = contact["email"]
            contact_dict["Telefono"] = contact["phone"]

            resp = json.loads(attributes.text)
            _dict = {}
            for contact in resp["fieldValues"]:
                # print(custom_fields[contact["field"]])
                if contact["field"]:
                    contact_dict[custom_fields[contact["field"]]] = contact["value"]
                if contact["field"] in ["8", "9"]:
                    _dict[contact["field"]] = contact["id"]

            ids_dict.update(_dict)
            list_of_ids.append(ids_dict)

            list_of_contacts.append(contact_dict)

        # print(list_of_ids)

        data = pd.DataFrame.from_records(list_of_contacts)

        # remove test users created with email: *@formulacoach.it before saving
        data = data[~data["Email"].str.contains("formulacoach.it")]

        print(data)

        to_save = data.set_index("Email")
        to_save = to_save[["SALES_CHANNEL"]]

        # save IteraContacts as JSON file
        # with open("./temp/itera_contacts_missing.csv", "w") as file:  # type: ignore
        #     json.dump(list_of_ids, file)

        # for later use when updating Customer Contacts
        # to_save.to_csv("./temp/itera_customers_missing.csv")

    except ApiException as err:
        print(f"ACTIVE: Exception when getting contacts from list : {err}\n")

except ApiException as err:
    print(f"ACTIVE: Exception when calling custom fields: {err}\n")
