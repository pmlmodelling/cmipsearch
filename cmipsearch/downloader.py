

import urllib.request
import copy
import os

def download_all(df, overwrite = False):
    """
    Download files
    Download all files in dataframe generated by cmip6_search

    Parameters
    -------------
    df: pandas dataframe
        A dataframe generated by cmip6_search
    overwrite: boolean
        Do you want to overwrite file if it already exists? Defaults to False.

    """

    files = list(copy.deepcopy(df.file))
    print(files)

    for i in range(0, len(df)):
        if df.file[i] in files:

            if overwrite:
                urllib.request.urlretrieve(df.url[i], df.file[i])
                files.remove(df.file[i])
            else:
                if os.path.exists(df.file[i]):
                    print(f"not overwriting {df.file[i]}")
                else:
                    urllib.request.urlretrieve(df.url[i], df.file[i])
                    files.remove(df.file[i])





