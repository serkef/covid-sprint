import json
import logging
import re
import subprocess
from typing import Dict, Tuple

import pandas as pd

OCCRP_COMPANIES = "../data/companies_final.csv"
OPENOWNERSHIP_COMPANIES = "../data/statements.latest.jsonl"

logging.basicConfig(
    format="%(asctime)s - %(levelname)s:%(message)s", level=logging.DEBUG
)


def get_occrp_companies() -> pd.DataFrame:
    return pd.read_csv(OCCRP_COMPANIES)


def normalize_company(company_name: str) -> str:
    return re.sub(r"[\W]", "", company_name).lower()


def count_lines():
    out = subprocess.run(["wc", "-l", OPENOWNERSHIP_COMPANIES], capture_output=True)
    return int(out.stdout.partition(b" ")[0])


def extract_open_ownership_info(company: Dict) -> Tuple:
    name = company.get("name", "")
    try:
        oc_link = [
            x["uri"]
            for x in company["identifiers"]
            if x["schemeName"] == "OpenCorporates"
        ][0]
    except (KeyError, IndexError) as exc:
        oc_link = ""
    try:
        oo_link = [
            x["uri"]
            for x in company["identifiers"]
            if x["schemeName"] == "OpenOwnership Register"
        ][0]
    except (KeyError, IndexError) as exc:
        oo_link = ""
    founding_date = company.get("foundingDate", "")
    try:
        address = company["addresses"][0]["country"]
    except (KeyError, IndexError) as exc:
        address = ""
    return name, oc_link, oo_link, founding_date, address


def enrich_company(companies: pd.DataFrame) -> pd.DataFrame:
    covid_companies = {}
    for company in companies.itertuples():
        company_name = normalize_company(company.supplier)
        if not company_name or company_name in covid_companies:
            continue
        covid_companies[company_name] = company.Index
    logging.info(f"Will search for {len(covid_companies):,} companies.")

    logging.info(f"Reading company data from {OPENOWNERSHIP_COMPANIES}")
    lines = count_lines()
    found = 0
    with open(OPENOWNERSHIP_COMPANIES, "r") as json_in:
        for idx, line in enumerate(json_in):
            company = json.loads(line)
            name = normalize_company(company.get("name", ""))
            if name in covid_companies:
                (
                    oo_name,
                    oc_link,
                    oo_link,
                    oo_fund_date,
                    oo_address,
                ) = extract_open_ownership_info(company)
                idx = covid_companies[name]
                companies.at[idx, "open_ownership_name"] = oo_name
                companies.at[idx, "open_corporate_link"] = oc_link
                companies.at[idx, "open_ownership_link"] = oo_link
                companies.at[idx, "open_ownership_founding_date"] = oo_fund_date
                companies.at[idx, "open_ownership_address_country"] = oo_address
                found += 1
            if idx % 100_000 == 0:
                logging.info(
                    f"Read: {idx:,} {idx * 100 / lines:.0f}%. "
                    f"Found {found:,} {found * 100 / len(covid_companies):.0f}%"
                )
    return companies


def main():
    companies = get_occrp_companies()
    companies = enrich_company(companies)
    companies.to_csv("../data/companies_enriched.csv")


if __name__ == "__main__":
    main()
