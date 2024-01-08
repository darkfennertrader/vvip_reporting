import os
import json
from pprint import pprint
import requests
import pandas as pd
from dotenv import load_dotenv
from sib_api_v3_sdk.rest import ApiException


load_dotenv()


def _get_leads_from_active():
    print("\nGETTING DATA FROM ITERA DIGITAL...")
    headers = {"accept": "application/json", "Api-Token": os.getenv("active_api")}

    ## build dict with custom fields
    url = str(os.getenv("active_url")) + "fields"
    try:
        response = requests.get(url, headers=headers, timeout=30)
        resp = json.loads(response.text)
        custom_fields = {}
        for field in resp["fields"]:
            # print(f"{field['id']}: {field['perstag']}")
            custom_fields[field["id"]] = field["perstag"]

        # # save custom fields for later contacts update
        # with open("./data/itera_custom_fields.json", "w") as file:
        #     json.dump(custom_fields, file)

        # download contacts from list
        # url = "https://virtualvip.api-us1.com/api/3/contacts?listid=1"
        overall = pd.DataFrame()
        ids_list = []

        # priority of lists:
        # list_id = 1: "VirtualVIP - all Leads"
        # list_id = 4: "VirtualVIP - WhatsApp"
        # list_id = 3: "VirtualVIP - Calendly Leads"
        # list_id = 2: "VirtualVIP - all Leads Partial"

        # for list_id in [4, 1]:
        for list_id in [1, 4, 3, 2]:
            url = str(os.getenv("active_url")) + f"contacts?listid={list_id}&limit=100"
            page = 0
            overall_contacs = []
            while True:
                try:
                    url_get = url + f"&offset={page}"
                    response = requests.get(url_get, headers=headers, timeout=30)
                    resp = json.loads(response.text)
                    # print("*" * 80)
                    # # print(resp)
                    # print(len(resp["contacts"]))
                    # print("*" * 80)

                    overall_contacs.extend(resp["contacts"])

                    if len(resp["contacts"]) == 0:
                        break
                    page += 100
                except ApiException as error:
                    print(
                        f"ACTIVE: Exception when getting contacts from list : {error}\n"
                    )

            # print("*" * 80)
            # print(overall_contacs)
            # print(len(overall_contacs))
            # print("*" * 80)

            list_of_contacts = []
            list_of_ids = []

            for contact in overall_contacs:
                ids_dict = {}
                contact_dict = {}
                url = (
                    str(os.getenv("active_url"))
                    + f"contacts/{contact['id']}/fieldValues"
                )

                try:
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
                            contact_dict[custom_fields[contact["field"]]] = contact[
                                "value"
                            ]
                        if contact["field"] in ["8", "9"]:
                            _dict[contact["field"]] = contact["id"]

                    ids_dict.update(_dict)
                    list_of_ids.append(ids_dict)

                    list_of_contacts.append(contact_dict)

                except ApiException as err:
                    print(f"ACTIVE: Exception when getting attributes {err}\n")

            # print("*" * 80)
            # print(list_of_contacts)
            # print(len(list_of_contacts))
            # print("*" * 80)

            # print(len(list_of_ids))
            ids_list.append(list_of_ids)
            data = pd.DataFrame.from_records(list_of_contacts)

            # remove test users created with email: *@formulacoach.it before saving
            data = data[~data["Email"].str.contains("formulacoach.it")]
            data = data.set_index("Email")

            if list_id == 1:
                data["Form"] = "complete"
            if list_id == 2:
                data["Form"] = "missing"
            elif list_id == 3:
                data["Form"] = "calendly"
            elif list_id == 4:
                data["Form"] = "whatsup"
            # print(data)
            overall = pd.concat([overall, data])

        # make it compatible with old code
        _l = []
        for sublist in ids_list:
            _l.extend(sublist)

        # print(list(_l))
        # print(len(_l))

        # # save IteraContacts as JSON file
        # with open(os.getenv("itera_contacts_missing"), "w") as file:  #    type: ignore
        #     json.dump(_l, file)

        # print(overall)
        to_check = overall.sort_index()
        if to_check.index.has_duplicates:
            print("WARNING: duplicated records were deleted considering list priority")
            # print(to_check[to_check.index.duplicated()])
            df_reset = overall.reset_index()
            print("\nbefore removing:")
            print(df_reset.head(15))
            print(df_reset.shape)
            df_reset.drop_duplicates(subset=["Email"], keep="first", inplace=True)
            df_reset.set_index("Email", inplace=True)
            # print("\nafter removing:")
            # print(df_reset.head(15))
            overall = df_reset

            # contacts_to_del = list(to_check[to_check.index.duplicated()].index)
            # overall.drop_duplicates(index=contacts_to_del, inplace=True, keep="last")

        # to_save = overall[["SALES_CHANNEL"]]
        # # for later use when updating Customer Contacts
        # to_save.to_csv(os.getenv("itera_customers_missing"))

        return overall

    except ApiException as err:
        print(f"ACTIVE: Exception when getting fields : {err}\n")


if __name__ == "__main__":
    contacts = _get_leads_from_active()
    print(contacts.head(15))
    print(contacts.shape)
