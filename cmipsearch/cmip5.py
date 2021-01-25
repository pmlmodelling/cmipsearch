

# Keep an eye on this:
# https://pcmdi.llnl.gov/CMIP6/ArchiveStatistics/esgf_data_holdings/ScenarioMIP/index.html

import signal
import time
from contextlib import contextmanager

class TimeoutException(Exception): pass


@contextmanager
def time_limit(seconds):
    def signal_handler(signum, frame):
        raise TimeoutException("Timed out!")
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)



import os
import pandas as pd
import urllib3
http = urllib3.PoolManager()
from cmipsearch.models import cmip6_models


# List of possible mirrors

url_list1 = [
		"http://esgf.nccs.nasa.gov",
		"http://esgf-node.llnl.gov",
		"http://esgf-node.ipsl.upmc.fr",
		"http://esg-dn1.nsc.liu.se",
		"http://esgf-data.dkrz.de",
		"http://esgf-index1.ceda.ac.uk",
		"http://esg.pik-potsdam.de",
		"http://esgf.nci.org.au"
			   ]
# available frequencies
cmip_freq=['1hr','1hrCM','3hr','3hrPt','6hr','6hrPt','day','dec','fx','mon','monC','monPt','month','subhrPt','yr','yrPt']

def cmip5_search(models = "all",
        var = None,
        year_range = None,
        frequency = "mon",
        variant = "all",
        experiment = ["historical", "rcp26", "rcp45" , "rcp60", "rcp85"],
        wait = 60,
        url_list = "default"
        ):

    if url_list == "default":
        url_list = url_list1

    """
    Search for CMIP6 data

    Parameters
    -------------
    models: list or str
        Models to download. Defaults to "all", which results in all available models being downloaded. For a list of models call cmip6_models.
    var: str
        Variable to search for.
    frequency: str
        Time frequency to search for. Monthly is "mon". Daily is "day".
    year_range: list
        Two variable list giving the minimum and maximum year required. If these are not provided, all years will be searched for.
    variant: str
        Variant id to search for. Defaults to searching for all.
    experiment: list or str
        Experiment(s) to search for. Defaults to searching for all historical and SSP scenarios.
    wait: int
        Time to wait (in seconds) for each node to responds. Defaults to 60.


    Returns:
    pandas.DataFrame: Pandas dataframe showing available CMIP6 files matching search criteria.

    -------------

    """

    if year_range is not None:
        run_start = year_range[0]
        run_end= year_range[1]

    if models != "all":
        if type(models) is str:
            models = [models]

    #if models != "all":
    #    for model in models:
    #        if model not in cmip6_models():
    #            raise ValueError(f"{model} is not a valid CMIP5 model!")

    if frequency not in cmip_freq:
        raise ValueError(f"{frequency} is not a valid frequency!")

    tracker = 1
    files = []
    urls = []
    check_sums = []

    for url in url_list:

        new_url = url + "/esg-search/wget?project=CMIP5"

        if models != "all":
            for model in models:
                new_url = f"{new_url}&model={model}"

        if var is not None:
            new_url = f"{new_url}&variable={var}"

        new_url = f"{new_url}&time_frequency={frequency}"
        for ee in experiment:
            new_url = f"{new_url}&experiment={ee}"

        if variant != "all":
            if type(variant) is str:
                variant = [variant]
            for vv in variant:
                new_url = f"{new_url}&ensemble={vv}"

        new_url =f"{new_url}&limit=10000"

        try:
            with time_limit(wait):
                response = http.request("GET", new_url )

            lines = response.data.decode("utf-8").split("\n")
            tracker += 1

            change_level = 1

            for line in lines:

                if change_level == 2 and "dataset.file.url" in line:
                    change_level = 3

                if change_level == 2:
                    orig_line = line
                    years_avail = line[orig_line.index(".nc") -13:orig_line.index(".nc")]
                    year_start = int(years_avail[:4])
                    year_end = int(years_avail[7:11])
                    # maybe remove this if statement later....
                    if var in orig_line:
                        if year_range is not None:
                            if (year_start <= run_end and year_end >= run_start) or (year_end >= run_start and year_end <= run_end):
                                files.append(line.split(" ")[0])
                                urls.append(line.split(" ")[1])
                                check_sums.append(line.split(" ")[3])
                        else:
                            files.append(line.split(" ")[0].replace("'", ""))
                            urls.append(line.split(" ")[1].replace("'", ""))
                            check_sums.append(line.split(" ")[3])


    	    # Now pull out the years

                if change_level == 1:
                    if "download_files" in line:
                        change_level = 2

        except TimeoutException as e:
            print(f"The node {url} was slow to respond, and was ignored.")

    files = [x.replace("'", "") for x in files]
    urls = [x.replace("'", "") for x in urls]
    nodes = [x.replace("http://", "").split("/")[0] for x in urls]
    models = [x.split("_")[2] for x in files]
    variants = [x.split("_")[4] for x in files]
    experiments = [x.split("_")[3] for x in files]
    variables = [x.split("_")[0] for x in files]
    starts = [int(x.split("_")[-1][:4]) for x in files ]
    ends = [int(x.split("_")[-1].split("-")[1][:4]) for x in files ]
    df = pd.DataFrame({"variable":variables,"experiment":experiments, "model":models, "file":files, "url":urls, "node":nodes, "variant":variants, "start":starts, "end":ends, "checksum":check_sums}).drop_duplicates().reset_index(drop = True)

    return df






