import os
import asyncio
import aiohttp
from aiohttp import ClientError
import pandas as pd
import boto3
from dotenv import load_dotenv

load_dotenv()

# To do: Download data directly from DEMIO


username = os.getenv("username")
password = os.getenv("password")
url = os.getenv("prod_url") + os.getenv("promo_url")
client_id = os.getenv("prod_client_id")

filepath = "webinar_data/web_april_22_2024.csv"


def authenticate(client_id) -> str:
    print("\nVirtualVIP Backoffice data")
    print("*" * 30)
    client = boto3.client("cognito-idp", region_name="eu-west-1")

    # Initiating the Authentication,
    response = client.initiate_auth(
        ClientId=client_id,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={
            "USERNAME": username,
            "PASSWORD": password,
        },
    )

    return response["AuthenticationResult"]["IdToken"]


async def update_contact(
    id_token: str, session: aiohttp.client.ClientSession, emails: list[str]
):
    # update backoffice asynchronously
    promo_url = url
    headers = {"Authorization": f"Bearer {id_token}"}

    list_to_update = []
    for email, vat in emails:
        json_contact = {
            "email": email,
            "type": "business",
            "promo": "promo30",
            # values: "durata7", "durata30"
            "duration": "durata7",
            "vat": vat,
        }

        list_to_update.append(json_contact)

    json_data = {"request": list_to_update}
    print(json_data)

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


def update_backoffice(filename: str) -> None:
    data = pd.read_csv(filename)
    data = data[["Email", "Partiva IVA"]]
    # backoffice cannot handle None values
    data["Partiva IVA"] = data["Partiva IVA"].fillna("")
    # promo_list = data["Email"].to_list()
    promo_list = list(zip(data["Email"], data["Partiva IVA"]))
    # print(promo_list)
    # promo_list = promo_list[:10]

    id_token = authenticate(os.getenv("prod_client_id"))
    # # Run the main coroutine to update contacts
    result = asyncio.run(update_contacts(id_token, promo_list))

    print(f"\nResponse from update backoffice: {result}")


if __name__ == "__main__":
    update_backoffice(filepath)
