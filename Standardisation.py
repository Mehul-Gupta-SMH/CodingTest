import pandas as pd
from functools import lru_cache

@lru_cache
def std_git_license(value: str) -> str:
    """
    Standardises the license value based on the value provided
    :param value:
    :return:
    """
    license_type_df = pd.read_csv("GitLicensesVariants_fromOfficialGithubDocs.csv")

    # print(license_type_df.head())

    license_type_df["License"] = license_type_df["License"].apply(lambda x: x.lower())

    # print(license_type_df.head())

    filtered_license_cd = license_type_df[license_type_df["License"] == value.lower()]["License keyword"]

    # print(filtered_license_cd)

    if len(filtered_license_cd):
        return filtered_license_cd.iloc[0]
    else:
        return value




def process_category(value: str, type: str) -> str:
    """
    Standardise the cateorical value based on the category type
    :param value: Value that needs to be standardised
    :param type: Category type of standardisation
    :return: Standardised column value
    """
    if type == "clean_license":
        return std_git_license(value)
    else:
        return value

