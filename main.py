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
from send_emails import send_email_to_recipients


load_dotenv()


class DataLeads:
    mktg_actions = {
        "Business_Sales": "Sales",
        "Control": "Sales",  # Advertising
        "Online": "Sales",  # Promo + Advertising
        "Test": "",
    }

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

        # #########   UPDATE Campaigns   ########
        self._update_neting(final_dataset)
        self._update_itera(final_dataset)

        #########   Generate Reporting   ########
        report = self._output_reports(final_dataset)

    def _authenticate(self) -> str:
        print("\nVirtualVIP Backoffice data")
        print("*" * 30)
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
            ids_list = []
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

                    # print(len(list_of_ids))
                    ids_list.append(list_of_ids)
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

            # make it compatible with old code
            _l = []
            for sublist in ids_list:
                _l.extend(sublist)

            # print(list(_l))
            # print(len(_l))

            # save IteraContacts as JSON file
            with open(
                os.getenv("itera_contacts_missing"), "w"
            ) as file:  #    type: ignore
                json.dump(_l, file)

            # print(overall)
            to_check = overall.sort_index()
            if to_check.index.has_duplicates:
                print("WARNING DUPLICATED RECORDS IN ITERA LISTS")
                raise ValueError("ITERA data contain duplicated emails !!!")

            to_save = overall[["SALES_CHANNEL"]]
            # for later use when updating Customer Contacts
            to_save.to_csv(os.getenv("itera_customers_missing"))

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
                if isinstance(url_get, str):
                    pass
                else:
                    raise ValueError("returned none value")

                data = self._brevo_data_pre_processing(data, url_get)
                dataframe = pd.concat([dataframe, data])

            except ApiException as err:
                print(f"Exception when calling get_contacts_from_list: {err}\n")

        to_check = dataframe.sort_index()
        if to_check.index.has_duplicates:
            print("WARNING DUPLICATED RECORDS IN NETING LISTS")
            print(to_check[to_check.index.duplicated()])
            print()
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
        # check duplicates
        to_check = overall.sort_index()
        if to_check.index.has_duplicates:
            print("WARNING DUPLICATED RECORDS IN OVERALL  DATASET")
            raise ValueError("ITERA data contain duplicated emails !!!")

        print("\nOVERALL LEADS")
        print("*" * 30)
        print(overall.head())
        print(f"\nDATASET SHAPE: {overall.shape}")
        return overall

    def _get_vvip_reporting(self, overall):
        id_token = self._authenticate()
        vvip_data = self._get_vvip_data(id_token)

        idx_overall = overall.index

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

        idx_leads = leads.index

        print("\n\nCOMMERCIAL LEADS:")
        print("*" * 50)
        print(leads.head())
        print(f"\nLEADS SHAPE: {leads.shape}")
        print("*" * 50)

        #####################################################
        # (2) Customers with an active Free Trial
        #####################################################
        mask = (vvip_data["Customer_flag"] == "Free Trial") & (
            vvip_data["Expiration_date"] >= datetime.now().date()
        )
        # print(datetime.now().date())
        data_trial = vvip_data[mask]

        active_trials = overall.merge(
            data_trial, how="inner", left_index=True, right_index=True, indicator=True
        )

        active_trials = active_trials[
            ["Agency", "Subscription_date", "Expiration_date", "RCT_group"]
        ]
        active_trials["Status"] = "Active_Trials"

        idx_active_trials = active_trials.index

        print("\n\nACTIVE TRIALS:")
        print("*" * 50)
        print(active_trials.head())
        print(f"\nACTIVE TRIALS SHAPE: {active_trials.shape}")
        print("*" * 50)

        #####################################################
        # (3) Customers with a Free Trial expired
        #####################################################
        mask = (vvip_data["Customer_flag"] == "Free Trial") & (
            vvip_data["Expiration_date"] < datetime.now().date()
        )
        data_expired_trial = vvip_data[mask]

        expired_trials = overall.merge(
            data_expired_trial,
            how="inner",
            left_index=True,
            right_index=True,
            indicator=True,
        )

        expired_trials = expired_trials[
            ["Agency", "Subscription_date", "Expiration_date", "RCT_group"]
        ]
        expired_trials["Status"] = "Trials_Expired"

        idx_expired_trials = expired_trials.index

        print("\nEXPIRED TRIALS:")
        print("*" * 50)
        print(expired_trials.head())
        print(f"\nEXPIRED TRIALS SHAPE: {expired_trials.shape}")

        print("*" * 50)

        #####################################################
        # (4) Customers with Subscription
        #####################################################

        condition_1 = (
            (vvip_data["Customer_flag"] == "Business")
            & (vvip_data["Expiration_date"] >= datetime.now().date())
            & (pd.isna(vvip_data["Renewal_date"]))
        )

        condition_2 = (
            (vvip_data["Customer_flag"] == "Business")
            & (pd.isna(vvip_data["Expiration_date"]))
            & (vvip_data["Renewal_date"] >= datetime.now().date())
        )

        mask = condition_1 | condition_2
        data_subscriptions = vvip_data[mask]

        subscriptions = overall.merge(
            data_subscriptions,
            how="inner",
            left_index=True,
            right_index=True,
            indicator=False,
        )

        # subscriptions = subscriptions[
        #     ["Agency", "Subscription_date", "Expiration_date", "RCT_group"]
        # ]
        subscriptions["Status"] = "Subscriptions"

        idx_subscriptions = subscriptions.index

        print("\n\nACTIVE SUBSCRIPTIONS:")
        print("*" * 50)
        print(subscriptions.head())
        print(f"\nSUBSCRIPTIONS SHAPE: {subscriptions.shape}")

        print("*" * 50)

        #####################################################
        # checks whether or not there are non-classified data
        idx_diff = idx_overall.difference(idx_leads)
        idx_diff = idx_diff.difference(idx_active_trials)
        idx_diff = idx_diff.difference(idx_expired_trials)
        idx_diff = idx_diff.difference(idx_subscriptions)
        if idx_diff.to_list():
            raise ValueError(
                f"\nWARNING: There are some non classified customers. Check your code! {idx_diff.to_list()}"
            )

        overall_df = pd.concat([leads, active_trials, expired_trials, subscriptions])
        overall_df.sort_index(ascending=True, inplace=True)

        # check if there are inconsistencies in the Channel with Subscriptions between: "RCT_group" and "SalesChannel"
        # remove NaNvalues from "SalesChannel" since these Leads have not been converted yet
        subscr_check = overall_df.dropna(subset=["SalesChannel"])
        if not subscr_check["RCT_group"].equals(subscr_check["SalesChannel"]):
            print(
                "\nWARNING: Some channels assigned through RCT differ from some that were used to subscribe VVIP. Check the dataframe!!!\n"
            )

        overall_df = overall_df[["Status"]]

        # print(overall)
        # print(overall_status)

        final = pd.merge(overall, overall_df, left_index=True, right_index=True)

        return final

    def _update_itera(self, data):
        print("\nUPDATING ITERA_DIGITAL CONTACTS...")
        print("*" * 60)
        itera = data.loc[data["Agency"] == "itera"][["RCT_group", "Status"]]
        # print(itera, "\n")

        # load custom fields attributes
        with open("./data/itera_custom_fields.json", "r") as file:
            custom_fields = json.load(file)

        # load Customer id attributes
        with open("./data/itera_contacts_missing.json", "r") as file:
            itera_contacts = json.load(file)

        # load Customer "SALES_CHANNEL"
        itera_customers = pd.read_csv(
            "./data/itera_customers_missing.csv", index_col=["Email"]
        )

        # always update field "LEAD STATUS"
        update_lead_status = pd.merge(
            itera, itera_customers, left_index=True, right_index=True
        )
        update_lead_status.drop(columns=["SALES_CHANNEL"], inplace=True)
        final_leads = update_lead_status.to_dict("index")
        print(update_lead_status)
        key_lead_status = list(
            filter(lambda x: custom_fields[x] == "LEAD_STATUS", custom_fields)
        )[0]

        # updating attributes
        headers = {"accept": "application/json", "Api-Token": os.getenv("active_api")}
        for k, v in final_leads.items():
            # select the right customer
            contact = {}
            for elem in itera_contacts:
                if k in elem:
                    contact = elem
            print("\nupdating 'Lead Status' field of contact:")
            print(contact)
            try:
                # updating the Lead Status field
                url_lead_status = str(os.getenv("active_url")) + "fieldValues"
                payload = {
                    "fieldValue": {
                        "contact": contact[k],
                        "field": key_lead_status,  # 9
                        "value": v["Status"],
                    },
                    "useDefaults": False,
                }
                resp_status = requests.post(
                    url_lead_status, json=payload, headers=headers, timeout=30
                )
                if resp_status.status_code == 204:
                    print("\n CONTACTS UPDATED CORRECTLY\n")
                elif resp_status.status_code == 404:
                    print("\nERROR: some email(s) is/are not correct\n")

            except Exception as err:
                print(f"Exception when calling updating contacts: {err}\n")

        print("-" * 60)

        # update only those Customers that have a NaN value in the "SALES_CHANNEL" column
        to_update = itera_customers[itera_customers["SALES_CHANNEL"].isnull()]
        print(to_update)
        # check that all indexes (Emails) are in the itera dataset
        error = to_update.index.difference(itera.index)
        if list(error):
            raise ValueError(f"ERROR in the ITERA dataset: {error}")

        # result = pd.concat([itera, to_update], axis=1, join="outer")
        result = pd.merge(itera, to_update, left_index=True, right_index=True)
        result.drop(columns=["SALES_CHANNEL"], inplace=True)
        final = result.to_dict("index")
        print("\nITERA RECORDS TO UPDATE for field 'SALES CHANNEL':")
        pprint(final, indent=2)

        # Get ID of the custom field ("SALES_CHANNEL","LEAD_STATUS") to update
        key_sales_channel = list(
            filter(lambda x: custom_fields[x] == "SALES_CHANNEL", custom_fields)
        )[0]

        # updating attributes
        headers = {"accept": "application/json", "Api-Token": os.getenv("active_api")}
        for k, v in final.items():
            # select the right customer
            contact = {}
            for elem in itera_contacts:
                if k in elem:
                    contact = elem
            print("\nupdating 'Sales Channel' field of contact:")
            print(contact)

            # updating the Sales_Channel field
            url_sales_channel = str(os.getenv("active_url")) + "fieldValues"
            try:
                payload = {
                    "fieldValue": {
                        "contact": contact[k],
                        "field": key_sales_channel,  # 8
                        "value": self.mktg_actions[v["RCT_group"]],
                    },
                    "useDefaults": False,
                }
                print()
                resp_sales = requests.post(
                    url_sales_channel, json=payload, headers=headers, timeout=30
                )
                if resp_sales.status_code == 204:
                    print("\n CONTACTS UPDATED CORRECTLY\n")
                elif resp_sales.status_code == 404:
                    print("\nERROR: some email(s) is/are not correct\n")

            except Exception as err:
                print(f"Exception when calling updating contacts: {err}\n")

    def _update_neting(self, data):
        print("\nUPDATING NETING CONTACTS...")
        print("*" * 60)
        # print(data)
        # print(data.columns)
        # print(data.groupby(["Status"])["Agency"].count())
        neting = data.loc[data["Agency"] == "neting"][["RCT_group", "Status"]]

        print(neting, "\n")

        brevo_url = os.getenv("brevo_update_contacts_url")
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "api-key": os.getenv("brevo_api_key"),
        }

        payload = {}
        contacts_list = []
        for email in list(neting.index):
            _dict = {}
            _dict["attributes"] = {
                "Channel": self.mktg_actions[neting.loc[email].values[0]],
                "Status": neting.loc[email].values[1],
            }
            _dict["email"] = email
            contacts_list.append(_dict)

        payload["contacts"] = contacts_list
        print("\nBREVO Campaign Manager list:\n")
        pprint(payload, indent=1)

        # update Brevo Campaign Manager
        try:
            response = requests.post(
                brevo_url, json=payload, headers=headers, timeout=30  # type: ignore
            )
            print(response.text)
            if response.status_code == 204:
                print("\nCONTACTS UPDATED CORRECTLY\n")
            elif response.status_code == 404:
                print("\nERROR: some email(s) is/are not correct\n")
        except ApiException as err:
            print(f"Exception when update multiple contacts: {err}\n")

    def _output_reports(self, data: pd.DataFrame) -> pd.DataFrame:
        print("\nGENERATING REPORTING")
        print("*" * 30)
        print()
        print(data.head())
        data.to_csv("./output/customers.csv")
        out_rep = data.groupby(["Status", "Agency"])["Form"].count().unstack()
        print()
        print(out_rep)
        total_by_col = out_rep.sum(axis=1)
        total_by_col["Active_Trials"] = (
            0 if "Active_Trials" not in total_by_col.index else None
        )
        total_by_col["Subscribed"] = (
            0 if "Subscribed" not in total_by_col.index else None
        )
        redemption = (
            (total_by_col["Active_Trials"] + total_by_col["Trials_Expired"])
            / total_by_col.sum()
            * 100
        )

        print(f"\nOnline Redemption: {redemption:.2f}%")

        mask = (data["RCT_group"] == "Business_Sales") & (data["Form"] == "complete")
        sales_data = data.loc[mask]

        mask = (data["RCT_group"] == "Business_Sales") & (data["Form"] == "missing")

        sales_missing = data.loc[mask]

        output_file = "./channels/sales_" + str(datetime.now().date()) + ".xlsx"
        with pd.ExcelWriter(output_file) as excel_writer:
            sales_data.to_excel(excel_writer, sheet_name="Leads_w_complete_data")
            sales_missing.to_excel(excel_writer, sheet_name="Leads_w_missing_data")

        output_file = (
            "./channels/mktg_campaigns" + "_" + str(datetime.now().date()) + ".xlsx"
        )
        data.to_excel(output_file)

        return data  # type: ignore

    def _save_reports(self, report: pd.DataFrame) -> None:
        report.to_excel(os.getenv("report_filepath"))


def leads():
    data = pd.read_csv("./output/customers.csv")
    out_rep = data.groupby(["Status", "Agency"])["Form"].count().unstack()

    print()
    print(out_rep)
    print()
    total_by_col = out_rep.sum(axis=1)
    total_by_row = out_rep.sum(axis=0)
    print(total_by_row)
    print()
    print(total_by_col)

    total_by_col["Active_Trials"] = (
        0 if "Active_Trials" not in total_by_col.index else None
    )
    total_by_col["Subscribed"] = 0 if "Subscribed" not in total_by_col.index else None
    redemption = (
        (total_by_col["Active_Trials"] + total_by_col["Trials_Expired"])
        / total_by_col.sum()
        * 100
    )


if __name__ == "__main__":
    DataLeads()
    send_email_to_recipients()

    # Lead Stats
    # leads()
