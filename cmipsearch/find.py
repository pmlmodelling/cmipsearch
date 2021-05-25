import pandas as pd


all_df = pd.read_csv("cmipsearch/data/cmip6_variables.csv")


def find_variable(var = None, cmip = "cmip6"):
    """
    Search for available CMIP variables

    Parameters
    -------------
    var: str
        Character string to search for
    cmip: str
        Character string of the cmip being search. 'cmip6' is all that is available at present.


    Returns:
    pandas.DataFrame: Pandas dataframe showing available CMIP6 variables available

    -------------

    """


    df = all_df
    df["short"] = [x.lower() for x in df.Long_name]
    new_df = []
    for x in var.split(" "):
        x = x.lower()
        df = df.query("short.str.contains(@x)")
        df.columns = [x.lower() for x in df.columns]
    return df.drop(columns = "short").reset_index(drop = True)



