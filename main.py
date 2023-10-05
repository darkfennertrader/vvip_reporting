import os
from datetime import datetime
from pprint import pprint
import json
import numpy as np
import pandas as pd
import boto3
import requests
from dotenv import load_dotenv
from sib_api_v3_sdk.rest import ApiException
from leads_with_missing_data import MissingDataLeads
from randomized_control_trial import rct
from send_emails import send_email_to_recipients


pd.set_option("display.max_rows", None)
# pd.set_option("display.max_columns", None)
# pd.reset_option("display.max_columns")
# pd.reset_option("display.max_colwidth")Untitled
# pd.set_option("display.max_columns", 10)
# pd.set_option("display.max_colwidth", 1000)


# pd.options.display.width = 0
# pd.set_option("display.width", 500)


load_dotenv()

print(boto3.__version__)

client_id = os.getenv("prod_client_id")
username = os.getenv("username")
password = os.getenv("password")
url = os.getenv("prod_url")
get_url = os.getenv("get_url")
post_url = os.getenv("post_url")
agency_filepath = os.getenv("agency_filepath")
report_filepath = os.getenv("report_filepath")
itera_cust = os.getenv("itera_customers")
brevo_api = os.getenv("brevo_api_key")
brevo_update_url = os.getenv("brevo_update_contacts_url")
active_url = os.getenv("active_url")

if isinstance(url, str):
    pass
else:
    raise ValueError("returned none value")

if isinstance(get_url, str):
    pass
else:
    raise ValueError("returned none value")

if isinstance(post_url, str):
    pass
else:
    raise ValueError("returned none value")

if isinstance(agency_filepath, str):
    pass
else:
    raise ValueError("returned none value")


class VVIPReporting:

    """Class that generates VVIP reporting"""

    mktg_actions = {
        "Business_Sales": "Sales",
        "Control": "Advertising",
        "Online": "Promo + Advertising",
        "Test": "",
    }

    def __init__(self, url_address: str, get: str, post: str) -> None:
        self.client_id = client_id
        self.username = username
        self.password = password
        self.url = url_address
        self.get_url = get
        self.post_url = post
        id_token = self._authenticate()
        # print(id_token)
        raw_data = self._get_data(id_token)
        raw_data = self._get_raw_data(raw_data)
        agency_data, data = self._pre_processing(raw_data, agency_filepath)
        rep_data = self._reporting(agency_data, data)
        self._update_itera_campaign(rep_data)
        self._update_neting_campaign(rep_data)
        report = self._output_reports(rep_data)
        self._save_reports(report)

    def _authenticate(self) -> str:
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

    def _get_data(self, id_token: str) -> pd.DataFrame:
        resp = requests.get(
            url=self.url + self.get_url,
            headers={
                "Authorization": f"Bearer {id_token}",
            },
            timeout=30,
        )
        # pprint(resp.json(), indent=2)
        return pd.DataFrame.from_dict(resp.json()["Items"])

    def _get_raw_data(self, data: pd.DataFrame) -> pd.DataFrame:
        data.set_index("Customer_id", inplace=True)
        print("\nRAW DATA")
        print("*" * 20)
        print(data.head())
        print(data.shape)
        return data

    def _pre_processing(
        self, data: pd.DataFrame, agency_path: str | None
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        #### select fields from agency_data "
        if not agency_path:
            raise ValueError("env variable 'agency_path' is missing")

        agency_data = pd.read_csv(agency_path, index_col=0)
        agency_data.set_index("Email", inplace=True)
        # agency_data.index = agency_data.index.map(str.lower)
        agency_data.sort_index(inplace=True)

        if agency_data.index.has_duplicates:
            print("WARNING DUPLICATED RECORDS IN OUTPUT.CSV")
            print(agency_data.index)
            # is_duplicate = agency_data.index.duplicated(keep="first")
            # agency_data = agency_data[~is_duplicate]
            # print(agency_data)
            # print("." * 80)
            raise ValueError("Agency data contain duplicated emails !!!")
        else:
            print(agency_data)
            agency_data = agency_data[["Agency", "RCT_group"]].copy()
            print("\n\nPRE-PROCESSED AGENCY_DATA")
            print("*" * 30)
            print(agency_data.head(10))
            print(agency_data.shape)

        print()
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

        # rename column
        data.rename(columns={"Username": "Email"}, inplace=True)

        # filters out non Companies ("none" values)
        data = data[data["Company"] != "none"]
        # filter "Employee" value from "Customer_flag"
        data = data[data["Customer_flag"] != "Employee"]

        # converting from isoformat to datetime
        iso_lists = ["Subscription_date", "Expiration_date", "Renewal_date"]
        for date_col in iso_lists:
            data[date_col] = pd.to_datetime(
                data[date_col], format="ISO8601", errors="coerce"
            ).dt.date

        # remove duplicates with the same "Customer_id" (same customer whose subscription has expired multiple times). Keep last after ordering on "Subscription_date" column
        data.sort_values(by="Subscription_date", ascending=True, inplace=True)
        data = data[~data.index.duplicated(keep="last")]

        print("\n\nPRE-PROCESSED DATA")
        print("*" * 30)
        print(data.head(40))
        print(data.shape)

        return agency_data, data

    def _reporting(self, agency_data: pd.DataFrame, data: pd.DataFrame) -> pd.DataFrame:
        """Identify the different Customer Events:
        Lead --> Free Trial --> Trial Expired --> Subscription"""

        # prepare dataset for comparison with agency_data
        data.set_index("Email", inplace=True)
        data.sort_index(inplace=True)
        data.index = data.index.map(str.lower)

        print("\n\nREPORTING:")
        print("*" * 50)
        print(agency_data)
        print()
        print(data)
        print("*" * 50)
        #####################################################
        # (1) Identify Leads (Customers that have yet to activate a free trial)
        #####################################################
        mask = data["Customer_flag"] == "Free Trial"
        data_leads = data[mask]

        # email that are in "agency_data" and not in "data"
        leads = agency_data.merge(
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

        #####################################################
        # (2) Customers with an active Free Trial
        #####################################################
        mask = (data["Customer_flag"] == "Free Trial") & (
            data["Expiration_date"] >= datetime.now().date()
        )
        print(datetime.now().date())
        data_trial = data[mask]

        active_trials = agency_data.merge(
            data_trial, how="inner", left_index=True, right_index=True, indicator=True
        )

        active_trials = active_trials[
            ["Agency", "Subscription_date", "Expiration_date", "RCT_group"]
        ]
        active_trials["Status"] = "Active_Trials"

        print("\n\nACTIVE TRIALS:")
        print("*" * 50)
        print(active_trials)
        print("*" * 50)

        #####################################################
        # (3) Customers with a Free Trial expired
        #####################################################
        mask = (data["Customer_flag"] == "Free Trial") & (
            data["Expiration_date"] < datetime.now().date()
        )
        data_expired_trial = data[mask]

        expired_trials = agency_data.merge(
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

        print("\n\nEXPIRED TRIALS:")
        print("*" * 50)
        print(expired_trials)
        print("*" * 50)

        #####################################################
        # (4) Customers with Subscription
        #####################################################

        condition_1 = (
            (data["Customer_flag"] == "Business")
            & (data["Expiration_date"] >= datetime.now().date())
            & (pd.isna(data["Renewal_date"]))
        )

        condition_2 = (
            (data["Customer_flag"] == "Business")
            & (pd.isna(data["Expiration_date"]))
            & (data["Renewal_date"] >= datetime.now().date())
        )

        mask = condition_1 | condition_2
        data_subscriptions = data[mask]

        subscriptions = agency_data.merge(
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

        print("\n\nACTIVE SUBSCRIPTIONS:")
        print("*" * 50)
        print(subscriptions)
        print("*" * 50)

        #####################################################
        # Create Overall Dataset
        #####################################################

        overall = pd.concat([leads, active_trials, expired_trials, subscriptions])
        overall.sort_index(ascending=True, inplace=True)
        overall = overall[~overall.index.duplicated(keep="last")]

        # check if there are inconsistencies in the Channel with Subscriptions between: "RCT_group" and "SalesChannel"
        # remove NaNvalues from "SalesChannel" since these Leads have not been converted yet
        subscr_check = overall.dropna(subset=["SalesChannel"])
        if not subscr_check["RCT_group"].equals(subscr_check["SalesChannel"]):
            print(
                "\nWARNING: Some channels assigned through RCT differ from some that were used to subscribe VVIP. Check the dataframe!!!\n"
            )

        # reorder columns
        overall = overall[
            [
                "Agency",
                "RCT_group",
                # "SalesChannel",
                "Status",
                # "Customer_flag",
            ]
        ]

        overall["Customers"] = 1

        sales_update = overall[["Status"]]

        #### update SALES REPORT #######
        print("-" * 100)
        # print("UPDATE SALES REPORT")

        filepath = "./channels/sales" + "_" + str(datetime.now().date()) + ".csv"
        sales_data = pd.read_csv(filepath, index_col=["Email"])
        sales_update = overall[["Status"]]

        # print(overall)

        sales_data = pd.merge(
            sales_data, sales_update, left_index=True, right_index=True, how="left"
        )

        filepath = (
            "./channels/leads_w_missing_data"
            + "_"
            + str(datetime.now().date())
            + ".csv"
        )
        sales_missing = pd.read_csv(filepath, index_col=["Email"])
        # print("\nsales missing")
        # print(sales_missing)

        output_file = "./channels/sales_" + str(datetime.now().date()) + ".xlsx"
        with pd.ExcelWriter(output_file) as excel_writer:
            sales_data.to_excel(excel_writer, sheet_name="Leads_w_complete_data")
            sales_missing.to_excel(excel_writer, sheet_name="Leads_w_missing_data")

        #### update MARKETING REPORT #######
        mktg_data = pd.read_csv("./output/output.csv", index_col=["Email"])
        mktg_data.drop(["Unnamed: 0"], axis=1, inplace=True)

        output_file = (
            "./channels/mktg_campaigns" + "_" + str(datetime.now().date()) + ".xlsx"
        )

        mktg_data = pd.merge(
            mktg_data,
            overall[["Status"]],
            left_index=True,
            right_index=True,
            how="left",
        )

        # print(mktg_data)

        with pd.ExcelWriter(output_file) as excel_writer:
            mktg_data.to_excel(excel_writer, sheet_name="Leads_w_complete_data")
            sales_missing.to_excel(excel_writer, sheet_name="Leads_w_missing_data")

        print(mktg_data)

        print("-" * 100)
        print("\n\nOverall Dataset:")
        print("*" * 50)
        print(overall)
        print("*" * 50)

        return overall

    def _update_itera_campaign(self, data: pd.DataFrame):
        print("\nUPDATING ITERA_DIGITAL CONTACTS...")
        print("*" * 60)
        itera = data.loc[data["Agency"] == "itera"][["RCT_group", "Status"]]
        print(itera, "\n")

        # load custom fields attributes
        with open("./data/itera_custom_fields.json", "r") as file:
            custom_fields = json.load(file)

        # load Customer id attributes
        with open("./data/itera_contacts.json", "r") as file:
            itera_contacts = json.load(file)

        # # # load Customer "SALES_CHANNEL"
        itera_customers = pd.read_csv("./data/itera_customers.csv", index_col=["Email"])

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
                url_lead_status = str(active_url) + "fieldValues"
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
        # print(to_update)

        # print(to_update)
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

        # FAKE DICTIONARY ###################################################
        # final = {
        #     "rodancom@icloud.com": {
        #         "RCT_group": "Test",
        #         "Status": "",
        #     },
        #     "michele.sabbadini@gustochef.it": {
        #         "RCT_group": "Test",
        #         "Status": "",
        #     },
        #     "mirdita@gmail.com": {
        #         "RCT_group": "Test",
        #         "Status": "",
        #     },
        #     "Pietro.p@whitelist.pro": {
        #         "RCT_group": "Test",
        #         "Status": "",
        #     },
        # }
        ####################################################################
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
            url_sales_channel = str(active_url) + "fieldValues"
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

                # # updating the Lead Status field
                # url_lead_status = str(active_url) + "fieldValues"
                # payload = {
                #     "fieldValue": {
                #         "contact": contact[k],
                #         "field": key_lead_status,  # 9
                #         "value": v["Status"],
                #     },
                #     "useDefaults": False,
                # }
                # resp_status = requests.post(
                #     url_lead_status, json=payload, headers=headers, timeout=30
                # )
                # if resp_status.status_code == 204:
                #     print("\n CONTACTS UPDATED CORRECTLY\n")
                # elif resp_status.status_code == 404:
                #     print("\nERROR: some email(s) is/are not correct\n")

            except Exception as err:
                print(f"Exception when calling updating contacts: {err}\n")

    def _update_neting_campaign(self, data: pd.DataFrame):
        print("\nUPDATING NETING CONTACTS...")
        print("*" * 60)
        # print(data)
        neting = data.loc[data["Agency"] == "neting"][["RCT_group", "Status"]]
        # remove contacts already updated

        print(neting, "\n")

        brevo_url = brevo_update_url
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "api-key": brevo_api,
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
            print(response)
            print(response.text)
            if response.status_code == 204:
                print("\n CONTACTS UPDATED CORRECTLY\n")
            elif response.status_code == 404:
                print("\nERROR: some email(s) is/are not correct\n")
        except ApiException as err:
            print(f"Exception when update multiple contacts: {err}\n")

    def _output_reports(self, data: pd.DataFrame) -> pd.DataFrame:
        """This function outputs the Campaign Reports"""
        data.reset_index(inplace=True, drop=True)
        data2 = data.drop(columns=["Agency"])
        # print(data)
        df_report = data2.groupby(["RCT_group", "Status"]).agg("sum").unstack()
        df_report.columns = df_report.columns.droplevel()  # type: ignore

        if "Leads" not in df_report:
            df_report["Leads"] = np.nan
        if "Active_Trials" not in df_report:
            df_report["Active_Trials"] = np.nan
        if "Trials_Expired" not in df_report:
            df_report["Trials_Expired"] = np.nan
        if "Subscriptions" not in df_report:
            df_report["Subscriptions"] = np.nan

        #  res = res.astype("Int64")

        df_report = df_report[
            ["Leads", "Active_Trials", "Trials_Expired", "Subscriptions"]
        ]
        df_report.index.name = None  # remove index name
        df_report = df_report.rename_axis(None, axis=1)  # type: ignore  # remove columns index name

        print("\n\nREPORTING:")
        print("*" * 60)
        print(df_report)
        print("*" * 60)

        return df_report  # type: ignore

    def _save_reports(self, report: pd.DataFrame) -> None:
        report.to_excel(report_filepath)


if __name__ == "__main__":
    MissingDataLeads()
    rct()
    VVIPReporting(url, get_url, post_url)
    # send_email_to_recipients()
