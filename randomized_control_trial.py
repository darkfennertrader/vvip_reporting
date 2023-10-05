import os
import smtplib
import json
from email import encoders
from typing import Any
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import collections
from pprint import pprint
import pandas as pd
import numpy as np
from sib_api_v3_sdk.rest import ApiException
import requests

from dotenv import load_dotenv

load_dotenv()


#####   data from Agencies    #####
ITERA = "./data/itera.xlsx"
NETING = "./data/neting.xlsx"
INVALID = "./data/invalid.xlsx"
#####   Output from script   #####
ARCHIVE = "./output/global.csv"
OUTPUT = "./output/output.csv"
OUT_ITERA = "./channels/itera"
OUT_NETING = "./channels/neting"
OUT_SALES = "./channels/sales"
brevo_api = os.getenv("brevo_api_key")
brevo_url = os.getenv("brevo_url")

active_api = os.getenv("active_api")
active_url = os.getenv("active_url")
itera_customers = os.getenv("itera_customers")
itera_contacts = os.getenv("itera_contacts")


channels = {1: "Business_Sales", 2: "Online", 3: "Control"}
mktg_actions = {
    "Business_Sales": "Sales",
    "Control": "Advertising",
    "Online": "Promo + Advertising",
}


def create_or_read_archive(starting_dataset) -> pd.DataFrame:
    """read overall file if exist otherwise create one"""
    if os.path.exists(starting_dataset):
        dataframe = pd.read_csv(starting_dataset, index_col=0)
        return dataframe

    data = pd.DataFrame(
        index=["Idx"],
        columns=[
            "Nome",
            "Cognome",
            "Telefono",
            "Email",
            "Qualifica",
            "Settore",
            "Dimensione",
            "Formazione",
            "Domanda1",
            "Domanda2",
            "Domanda3",
            "Agency",
            "RCT_group",
        ],
    )
    print("\nDB_INITIALISED:\n")
    data.to_csv(starting_dataset, header=True)
    return pd.DataFrame()


def get_data_from_active() -> pd.DataFrame | None:
    print("\nGETTING DATA FROM ITERA DIGITAL...")
    headers = {"accept": "application/json", "Api-Token": os.getenv("active_api")}

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

        # download contacts from list
        # url = "https://virtualvip.api-us1.com/api/3/contacts?listid=1"
        url = str(active_url) + "contacts?listid=1"

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

            to_save = data.set_index("Email")
            to_save = to_save[["SALES_CHANNEL"]]

            # save IteraContacts as JSON file
            with open(itera_contacts, "w") as file:  # type: ignore
                json.dump(list_of_ids, file)

            # for later use when updating Customer Contacts
            to_save.to_csv(itera_customers)

            return data

        except ApiException as err:
            print(f"ACTIVE: Exception when getting contacts from list : {err}\n")

    except ApiException as err:
        print(f"ACTIVE: Exception when calling custom fields: {err}\n")


def active_data_pre_processing(data: pd.DataFrame | None):
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

    print(data)

    data.drop(
        columns=["SALES_CHANNEL", "LEAD_STATUS"], axis=1, errors="ignore", inplace=True
    )

    return data.to_excel(ITERA, index=False)


def get_data_from_brevo():
    print("\nGETTING DATA FROM NETING...")
    try:
        resp = requests.get(
            url=brevo_url,  # type: ignore
            headers={
                "accept": "application/json",
                "api-key": brevo_api,
            },  # type: ignore
            timeout=30,
        )
        print(resp.status_code)
        return json.loads(resp.text)

    except ApiException as err:
        print(f"Exception when calling get_contacts_from_list: {err}\n")


def brevo_data_pre_processing(data: Any):
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

    print(dataframe)

    dataframe.drop(columns=["CHANNEL", "STATUS"], axis=1, errors="ignore", inplace=True)

    return dataframe.to_excel(NETING, index=False)


def read_sources(
    itera_path: str, neting_path: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read sources for later processing"""
    itera_data = pd.read_excel(itera_path, engine="openpyxl")
    itera_data["Agency"] = "itera"

    neting_data = pd.read_excel(neting_path, engine="openpyxl")
    neting_data["Agency"] = "neting"

    return itera_data, neting_data


def _check_duplicates(data: pd.DataFrame) -> bool:
    data = data[data.index.duplicated(keep=False)]
    if not data.empty:
        print("\nDUPLICATED INDEXES:")
        print(data, "\n")
        return True
    return False


def _calculate_groups_percent(data: pd.DataFrame) -> None:
    # transform column to array
    rct_arr = data["RCT_group"].to_numpy()
    tot_samples = len(rct_arr)
    # getting the elements frequencies using Counter class
    elements_count = collections.Counter(rct_arr)
    elements_count = dict(sorted(collections.Counter(rct_arr).items()))  # type: ignore
    # printing the element and the frequency
    print("\n")
    print("*" * 30)
    for key, value in elements_count.items():
        print(f"{key}: {np.round(value/tot_samples*100, 2)}%")
    print("*" * 30, "\n")


def _mark_invalid_records(overall, itera, neting):
    # mark invalid records (due to mistakes, corrections, tests)
    print("\nINVALID RECORDS:")
    invalid = overall.set_index("Email")
    itera_checks = itera.set_index("Email")
    itera_checks["Agency"] = "itera"
    neting_checks = neting.set_index("Email")
    neting_checks["Agency"] = "neting"
    # print(overall_checks)
    idx_diff = invalid.index.difference(itera_checks.index)
    # print(idx_diff)
    invalid = invalid[invalid.index.isin(idx_diff)]
    idx_diff = invalid.index.difference(neting_checks.index)
    invalid = invalid[invalid.index.isin(idx_diff)]
    print(invalid)
    invalid.to_excel(INVALID)


def rct_assignment(
    overall: pd.DataFrame, itera: pd.DataFrame, neting: pd.DataFrame
) -> pd.DataFrame:
    """Randomically assign rolling commercial leads to : "Sales", "Online" or "Control" Group"""

    print("BEFORE CONCATENATION:")

    overall_aft = pd.concat([overall, itera, neting]).reset_index(drop=True)
    # print("\nBEFORE DROP DUPLICATES:")
    # print(overall_aft)
    overall_aft.drop_duplicates(ignore_index=True, inplace=True)
    overall_aft.sort_index(inplace=True)

    print("\nAFTER DROP DUPLICATES:")
    print(overall_aft)
    print("*" * 120)

    overall_itera = overall_aft.loc[overall_aft["Agency"] == "itera"]
    overall_neting = overall_aft.loc[overall_aft["Agency"] == "neting"]

    # eliminate test users created with email: *@formulacoach.it before RCT assignment
    overall_itera = overall_itera[
        ~overall_itera["Email"].str.contains("formulacoach.it")
    ]
    overall_neting = overall_neting[
        ~overall_neting["Email"].str.contains("formulacoach.it")
    ]

    # TO DO: eliminate invalid Leads if file exists before RCT assignment
    # check_file = os.path.isfile(INVALID)
    # if check_file:
    #     # read invalid records
    #     invalid_records = pd.read_excel(INVALID)
    #     invalid_records.set_index("Email", inplace=True)
    #     itera_mask = overall_itera["Email"].isin(list(invalid_records.index))
    #     neting_mask = overall_neting["Email"].isin(list(invalid_records.index))
    #     overall_itera = overall_itera.loc[~itera_mask]
    #     overall_neting = overall_neting.loc[~neting_mask]

    online_agencies = [overall_itera, overall_neting]
    rct_column = pd.DataFrame()
    for dataframe in online_agencies:
        samples = dataframe.shape[0]
        #     # assign customers to RCT groups
        np.random.seed(42)
        rct_list = []
        for _ in range(0, samples):
            # print(np.random.choice(np.arange(1, 4), p=[0.4, 0.4, 0.2]))
            rct_list.append(np.random.choice(np.arange(1, 4), p=[0.4, 0.4, 0.2]))

        # transpose numbers to group names
        rct_list = [channels.get(item, item) for item in rct_list]
        df_to_join = pd.DataFrame(
            index=dataframe.index, data=rct_list, columns=["RCT_group"]
        )
        rct_column = pd.concat([rct_column, df_to_join])

    rct_column.sort_index(inplace=True)

    overall_final = pd.merge(overall_aft, rct_column, left_index=True, right_index=True)
    print("\nFINAL DATASET:")
    print(overall_final)

    _calculate_groups_percent(overall_final)

    return overall_final


def save_data(data: pd.DataFrame) -> None:
    # print("\nwithin SAVE DATA:")
    # print(data)
    data.to_csv(OUTPUT, encoding="utf-8")
    # backup data in case of mistakes
    backup_file = "./backup/output_" + str(datetime.now().date()) + ".csv"
    data.to_csv(backup_file, encoding="utf-8")

    output_itera = data.loc[data["Agency"] == "itera"]
    output_neting = data.loc[data["Agency"] == "neting"]
    output_sales = data.loc[data["RCT_group"] == "Business_Sales"]

    itera = OUT_ITERA + "_" + str(datetime.now().date()) + ".csv"
    neting = OUT_NETING + "_" + str(datetime.now().date()) + ".csv"
    sales = OUT_SALES + "_" + str(datetime.now().date()) + ".csv"
    output_itera.to_csv(itera, index=False, encoding="utf-8")
    output_neting.to_csv(neting, index=False, encoding="utf-8")
    output_sales.to_csv(sales, index=False, encoding="utf-8")

    data.drop(columns="RCT_group", inplace=True)
    data.to_csv(ARCHIVE, encoding="utf-8")
    backup_file = "./backup/global_" + str(datetime.now().date()) + ".csv"
    data.to_csv(backup_file, encoding="utf-8")


def rct():
    """Main function that generates reporting"""
    overall = create_or_read_archive(ARCHIVE)
    if not overall.empty:
        print("\nSTARTING DATASET:")
        print(overall)
        print(overall.shape, "\n")

    # getting data from the Active Campaign Manager (IteraDigital)
    raw_data = get_data_from_active()
    active_data_pre_processing(raw_data)

    # getting data from the Brevo Campaign Manager (NETING)
    raw_data = get_data_from_brevo()
    brevo_data_pre_processing(raw_data)

    itera_data, neting_data = read_sources(ITERA, NETING)
    print("\nITERA Commercial Leads")
    print(itera_data)
    print(print("\nNETING Commercial Leads"))
    print(neting_data)
    overall_df = rct_assignment(overall, itera_data, neting_data)
    _mark_invalid_records(overall_df, itera_data, neting_data)
    save_data(overall_df)


if __name__ == "__main__":
    rct()
