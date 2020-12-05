from pytrends.request import TrendReq
import pandas as pd

keywords = ["covid", "coronavirus"]
country_codes = [
    "AT",
    "BE",
    "BG",
    "CY",
    "CZ",
    "DE",
    "DK",
    "EE",
    "ES",
    "FI",
    "FR",
    "GB",
    "GR",
    "HR",
    "HU",
    "IE",
    "IT",
    "LT",
    "LU",
    "LV",
    "MT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SE",
    "SI",
    "SK",
]


def process_df(df: pd.DataFrame, country_code) -> pd.DataFrame:
    # Drop partial weeks
    df = df[df["isPartial"] == "False"]
    df.drop(["isPartial"], axis=1, inplace=True)

    # Get max_search
    df["search_max"] = df.max(axis=1)

    # Prepare to merge
    df["country_code"] = country_code
    df.set_index("country_code", append=True, inplace=True)

    return df


def main():
    main_df = pd.DataFrame()
    for country_code in country_codes:
        pytrend = TrendReq(hl=country_code)
        pytrend.build_payload(
            kw_list=keywords, timeframe="today 12-m", geo=country_code
        )
        df = pytrend.interest_over_time()
        df = process_df(df, country_code)
        df.to_csv(f"covid_weekly_google_trends_{country_code}.csv")
        main_df = pd.concat((main_df, df))

    main_df.to_csv("covid_weekly_google_trends.csv")


if __name__ == "__main__":
    main()
