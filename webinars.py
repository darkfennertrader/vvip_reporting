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

from dotenv import load_dotenv
from send_emails import send_email_to_recipients


load_dotenv()


async def update_contact(
    id_token: str, session: aiohttp.client.ClientSession, emails: list[str]
):
    # update backoffice asynchronously
    promo_url = os.getenv("prod_url") + os.getenv("promo_url")  # type: ignore
    headers = {"Authorization": f"Bearer {id_token}"}

    list_to_update = []
    for email in emails:
        json_contact = {
            "email": email,
            "type": "business",
            "promo": "promo6030",
            "duration": "",
        }

        list_to_update.append(json_contact)

    json_data = {"request": list_to_update}
    # print()
    # pprint(json_data, indent=2)

    try:
        async with session.put(promo_url, json=json_data, headers=headers) as response:
            # You might want to check for other status codes here too
            if response.status == 429:  # Too Many Requests / Rate Limited
                print(f"Rate limit hit when updating: {list_to_update}. Retrying...")
                await asyncio.sleep(1)  # Wait for a second before retrying
                return await update_contact(id_token, session, emails)

            if response.headers.get("Content-Type") == "application/json":
                response_data = await response.json()
            else:
                response_data = await response.text()

            return response_data, response.status

    except ClientError as e:
        print(f"Network error occurred: {e}")
        return {"error": "Network error occurred"}, 500  # Example error response


async def update_contacts(id_token: str, emails: list[str]):
    async with aiohttp.ClientSession() as session:
        tasks = [update_contact(id_token, session, emails)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results


class IteraWebinars:

    def __init__(self) -> None:
        self.url = os.getenv("prod_url")
        self.get_url = os.getenv("get_url")
        self.post_url = os.getenv("post_url")
        self.client_id = os.getenv("prod_client_id")
        self.uat_client_id = os.getenv("uat_client_id")
        self.username = os.getenv("username")
        self.password = os.getenv("password")
        self.url = os.getenv("prod_url")
        self.get_url = os.getenv("get_url")

        #########   ITERA data   #############
        raw_data = self._get_leads_from_active()
        itera_data = self._active_data_pre_processing(raw_data)
        ######################################
        self._update_backoffice(itera_data)

    def _authenticate(self, client_id) -> str:
        print("\nVirtualVIP Backoffice data")
        print("*" * 30)
        client = boto3.client("cognito-idp", region_name="eu-west-1")

        # Initiating the Authentication,
        response = client.initiate_auth(
            ClientId=client_id,
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={
                "USERNAME": self.username,
                "PASSWORD": self.password,
            },
        )

        return response["AuthenticationResult"]["IdToken"]

    def _get_leads_from_active(self):
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

            # save custom fields for later contacts update
            with open("./itera/itera_custom_fields.json", "w") as file:
                json.dump(custom_fields, file)

            # download contacts from list
            # url = "https://virtualvip.api-us1.com/api/3/contacts?listid=1"
            overall = pd.DataFrame()
            ids_list = []

            # list priority:
            # list_id = 1: "VirtualVIP - all Leads"
            # list_id = 5: "VirtualVIP - Webinars Febbraio 2024"
            # list_id = 4: "VirtualVIP - WhatsApp"
            # list_id = 3: "VirtualVIP - Calendly Leads"
            # list_id = 2: "VirtualVIP - all Leads Partial"

            # for list_id in [4, 1]:
            for list_id in [5]:
                url = (
                    str(os.getenv("active_url"))
                    + f"contacts?listid={list_id}&limit=100"
                )
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

                if list_id == 5:
                    data["Form"] = "webinar_feb_2024"
                # print(data)
                overall = pd.concat([overall, data])

            # make it compatible with old code
            _l = []
            for sublist in ids_list:
                _l.extend(sublist)

            # print(list(_l))
            # print(len(_l))

            # save IteraContacts as JSON file
            with open("./itera/itera_contacts_missing", "w") as file:
                json.dump(_l, file)

            # print(overall)
            to_check = overall.sort_index()
            if to_check.index.has_duplicates:
                print(
                    "WARNING: duplicated records were deleted considering list priority"
                )
                # changed from to_check to overall to mantain order from lists
                df_reset = overall.reset_index()
                # print("\nbefore removing:")
                # print(df_reset.head(15))
                # print(df_reset.shape)
                df_reset.drop_duplicates(subset=["Email"], keep="first", inplace=True)
                df_reset.set_index("Email", inplace=True)
                # print("\nafter removing:")
                # print(df_reset.head(15))
                overall = df_reset

                # contacts_to_del = list(to_check[to_check.index.duplicated()].index)
                # overall.drop_duplicates(index=contacts_to_del, inplace=True, keep="last")

            to_save = overall[["SALES_CHANNEL"]]
            # for later use when updating Customer Contacts
            to_save.to_csv("./itera/itera_customers_missing")

            return overall

        except ApiException as err:
            print(f"ACTIVE: Exception when getting fields : {err}\n")

    def _active_data_pre_processing(self, data: pd.DataFrame | None):
        if isinstance(data, type(None)):
            raise ValueError("ERROR: dataframe is empty")

        # print(data, "\n")
        # print(data.columns)
        # renaming columns
        data.rename(
            columns={
                "TELEFONO": "Telefono",
                "QUALIFICA_AZIENDALE": "Qualifica",
                "SETTORE_AZIENDA": "Settore",
                # "DIMENSIONE_IMPRESA": "Impresa",
                # "FORMAZIONE": "Formazione",
                # "DOMANDA_1": "Domanda1",
                # "DOMANDA_2": "Domanda2",
                # "DOMANDA_3": "Domanda3",
            },
            inplace=True,
        )
        data["Agency"] = "itera"
        data["RCT_group"] = "Business_Sales"
        # data["Lead_Origin"] = "Linkedin"

        data.drop(
            columns=["SALES_CHANNEL", "LEAD_STATUS"],
            axis=1,
            errors="ignore",
            inplace=True,
        )

        data = data[
            [
                "Nome",
                "Cognome",
                "Telefono",
                "Qualifica",
                "Settore",
                # "Impresa",
                # "Formazione",
                # "Domanda1",
                # "Domanda2",
                # "Domanda3",
                "Form",
                "Agency",
                "RCT_group",
                # "Lead_Origin",
            ]
        ]
        print(data.head(30))
        print(data.shape)

        return data

    def _update_backoffice(self, data: pd.DataFrame) -> None:
        email_list = list(data.index)

        id_token = self._authenticate(self.client_id)
        # Run the main coroutine to update contacts
        result = asyncio.run(update_contacts(id_token, email_list))

        print(f"\nResponse from update backoffice: {result}")


if __name__ == "__main__":

    itera = IteraWebinars()
