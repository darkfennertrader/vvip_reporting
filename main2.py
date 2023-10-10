import os
import json
from typing import Any
from datetime import datetime
import collections
from pprint import pprint
import pandas as pd
import numpy as np
from sib_api_v3_sdk.rest import ApiException
import requests
import boto3

from dotenv import load_dotenv

load_dotenv()


class DataLeads:
    def __init__(self) -> None:
        self.url = os.getenv("prod_url")
        self.get_url = os.getenv("get_url")
        self.post_url = os.getenv("post_url")
        self.client_id = os.getenv("prod_client_id")
        self.username = os.getenv("username")
        self.password = os.getenv("password")
        self.url = os.getenv("prod_url")
        self.get_url = os.getenv("get_url")

        #########   ITERA data   #############
        raw_data = self._get_leads_from_active()
        itera_data = self._active_data_pre_processing(raw_data)

        #########   NETING data   #############
        neting_data = self._get_leads_from_brevo()

        overall = self._overall_customers(itera_data, neting_data)
        #########   VirtualVIP data   ########
        final_dataset = self._get_vvip_reporting(overall)
        # id_token = self._authenticate()
        # raw_data = self._get_data(id_token)
        # vvip_data = self._get_raw_data(raw_data)

        # final_dataset = self._get_vvip_reporting(overall)
        # self._update_itera_missing(final_dataset)
        # self._update_neting_missing(final_dataset)

    def _authenticate(self) -> str:
        print("\nVirtualVIP Backoffice data:")
        print("*" * 20)
        client = boto3.client("cognito-idp", region_name="eu-west-1")

        # Initiating the Authentication,
        response = client.initiate_auth(
            ClientId=self.client_id,
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={
                "USERNAME": self.username,
                "PASSWORD": self.password,
            },
        )

        return response["AuthenticationResult"]["IdToken"]

    def _get_vvip_data(self, id_token):
        resp = requests.get(
            url=self.url + self.get_url,  # type: ignore
            headers={
                "Authorization": f"Bearer {id_token}",
            },
            timeout=30,
        )
        # pprint(resp.json(), indent=2)
        data = pd.DataFrame.from_dict(resp.json()["Items"])
        data.set_index("Customer_id", inplace=True)

        data = data[
            [
                "Subscription_date",
                "Expiration_date",
                "Renewal_date",
                "SalesChannel",
                "Customer_flag",
                "Company",
                "Username",
            ]
        ].copy()

        # converting from isoformat to datetime
        iso_lists = ["Subscription_date", "Expiration_date", "Renewal_date"]
        for date_col in iso_lists:
            data[date_col] = pd.to_datetime(
                data[date_col], format="ISO8601", errors="coerce"
            ).dt.date

        data.sort_values(by="Subscription_date", ascending=True, inplace=True)
        data = data[~data.index.duplicated(keep="last")]

        data.set_index("Username", inplace=True)
        data.index.name = "Email"
        print(data.head())
        print(data.shape)
        return data

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
            with open("./data/itera_custom_fields.json", "w") as file:
                json.dump(custom_fields, file)

            # download contacts from list
            # url = "https://virtualvip.api-us1.com/api/3/contacts?listid=1"

            overall = pd.DataFrame()
            for list_id in [1, 2]:
                url = str(os.getenv("active_url")) + f"contacts?listid={list_id}"

                try:
                    response = requests.get(url, headers=headers, timeout=30)
                    resp = json.loads(response.text)

                    list_of_contacts = []
                    list_of_ids = []

                    for contact in resp["contacts"]:
                        ids_dict = {}
                        contact_dict = {}
                        url = (
                            str(os.getenv("active_url"))
                            + f"contacts/{contact['id']}/fieldValues"
                        )
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

                    # print(list_of_ids)

                    data = pd.DataFrame.from_records(list_of_contacts)

                    # remove test users created with email: *@formulacoach.it before saving
                    data = data[~data["Email"].str.contains("formulacoach.it")]
                    data = data.set_index("Email")
                    if list_id == 1:
                        data["Form"] = "complete"
                    elif list_id == 2:
                        data["Form"] = "missing"
                    # print(data)
                    overall = pd.concat([overall, data])

                except ApiException as err:
                    print(
                        f"ACTIVE: Exception when getting contacts from list : {err}\n"
                    )

            # print(overall)
            to_check = overall.sort_index()

            if to_check.index.has_duplicates:
                print("WARNING DUPLICATED RECORDS IN ITERA LISTS")
                raise ValueError("ITERA data contain duplicated emails !!!")

            return overall

        except ApiException as err:
            print(f"ACTIVE: Exception when calling custom fields: {err}\n")

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
                "DIMENSIONE_IMPRESA": "Impresa",
                "FORMAZIONE": "Formazione",
                "DOMANDA_1": "Domanda1",
                "DOMANDA_2": "Domanda2",
                "DOMANDA_3": "Domanda3",
            },
            inplace=True,
        )
        data["Agency"] = "itera"
        data["RCT_group"] = "Business_Sales"

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
                "Impresa",
                "Formazione",
                "Domanda1",
                "Domanda2",
                "Domanda3",
                "Form",
                "Agency",
                "RCT_group",
            ]
        ]
        print(data.head())
        print(data.shape)

        return data

    def _get_leads_from_brevo(self):
        print("\nGETTING DATA FROM NETING...")
        urls = [os.getenv("brevo_url"), os.getenv("brevo_url_missing")]
        dataframe = pd.DataFrame()
        for url_get in urls:
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

                data = self._brevo_data_pre_processing(data, url_get)
                dataframe = pd.concat([dataframe, data])

            except ApiException as err:
                print(f"Exception when calling get_contacts_from_list: {err}\n")

        to_check = dataframe.sort_index()
        if to_check.index.has_duplicates:
            print("WARNING DUPLICATED RECORDS IN NETING LISTS")
            raise ValueError("ITERA data contain duplicated emails !!!")

        print(dataframe.head())
        print(dataframe.shape)

        return dataframe

    def _brevo_data_pre_processing(self, data: Any, list_url: str):
        # print("BREVO PREPROCESSING")
        # pprint(data, indent=2)
        # pprint(data["contacts"])
        customer_list = []
        for elem in data["contacts"]:
            elem["attributes"]["Email"] = elem["email"]
            customer_list.append(elem["attributes"])

        # print(pd.DataFrame(customer_list))
        dataframe = pd.DataFrame(customer_list)

        dataframe = dataframe[~dataframe["Email"].str.contains("formulacoach.it")]

        # selecting a subset of columns:
        dataframe = dataframe[
            [
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
            ]
        ]

        # renaming columns
        dataframe.rename(
            columns={
                "FIRSTNAME": "Nome",
                "LASTNAME": "Cognome",
                "TELEFONO": "Telefono",
                "QUALIFICA": "Qualifica",
                "ATECO": "Settore",
                "IMPRESA": "Impresa",
                "FORMAZIONE": "Formazione",
                "TIPO_FORMAZIONE": "Domanda1",
                "ANNI_FORMAZIONE_AZIENDA": "Domanda2",
                "PENSIERO_SU_AI": "Domanda3",
            },
            inplace=True,
        )

        if list_url == os.getenv("brevo_url"):
            dataframe["Form"] = "complete"
        elif list_url == os.getenv("brevo_url_missing"):
            dataframe["Form"] = "missing"

        dataframe["Agency"] = "neting"
        dataframe["RCT_group"] = "Business_Sales"

        # print(dataframe.head())

        dataframe.drop(
            columns=["CHANNEL", "STATUS"], axis=1, errors="ignore", inplace=True
        )

        dataframe.set_index("Email", inplace=True)
        # print(dataframe.columns)

        return dataframe

    def _overall_customers(self, itera, neting):
        overall = pd.concat([itera, neting])
        overall.sort_index(inplace=True)
        print("\nOVERALL LEADS")
        print("*" * 30)
        print(overall.head())
        print(f"\nDATASET SHAPE: {overall.shape}")
        return overall

    def _get_vvip_reporting(self, overall):
        id_token = self._authenticate()
        vvip_data = self._get_vvip_data(id_token)

        #####################################################
        # (1) Identify Leads (Customers that have yet to activate a free trial)
        #####################################################
        mask = vvip_data["Customer_flag"] == "Free Trial"
        data_leads = vvip_data[mask]

        # email that are in "agency_data" and not in "data"
        leads = overall.merge(
            data_leads, how="left", left_index=True, right_index=True, indicator=True
        ).loc[lambda x: x["_merge"] != "both"]

        leads = leads[
            [
                "Agency",
                "Subscription_date",
                "Expiration_date",
                "RCT_group",
            ]
        ]
        leads["Status"] = "Leads"

        print("\n\nCOMMERCIAL LEADS:")
        print("*" * 50)
        print(leads)
        print("*" * 50)

        return None


if __name__ == "__main__":
    DataLeads()
