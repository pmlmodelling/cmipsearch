

# Keep an eye on this:
# https://pcmdi.llnl.gov/CMIP6/ArchiveStatistics/esgf_data_holdings/ScenarioMIP/index.html


import os
import pandas as pd
import urllib3
http = urllib3.PoolManager()
from cmipsearch.models import cmip6_models


# List of possible mirrors

url_list = [
		"http://esgf.nccs.nasa.gov",
		"http://esgf-node.llnl.gov",
		"http://esgf-node.ipsl.upmc.fr",
		"http://esg-dn1.nsc.liu.se",
		"http://esgf-data.dkrz.de",
		"http://esgf-index1.ceda.ac.uk",
		"http://esg.pik-potsdam.de",
		"http://esgf.nci.org.au"
			   ]
# available models...


#def cmip_models():
#    return ['AOP-v1-5','ACCESS-CM2','ACCESS-ESM1-5','ARTS-2-3','AWI-CM-1-1-HR','AWI-CM-1-1-LR','AWI-CM-1-1-MR','AWI-ESM-1-1-LR','BCC-CSM2-MR','BCC-ESM1','CAMS-CSM1-0','CAS-ESM2-0','CESM1-1-CAM5-CMIP5','CESM2','CESM2-FV2','CESM2-WACCM','CESM2-WACCM-FV2','CIESM','CMCC-CM2-HR4','CMCC-CM2-SR5','CMCC-CM2-VHR4','CMCC-ESM2','CNRM-CM6-1','CNRM-CM6-1-HR','CNRM-ESM2-1','CanESM5','CanESM5-CanOE','E3SM-1-0','E3SM-1-1','E3SM-1-1-ECA','EC-Earth3','EC-Earth3-LR','EC-Earth3-Veg','EC-Earth3-Veg-LR','EC-Earth3P','EC-Earth3P-HR','ECMWF-IFS-HR','ECMWF-IFS-LR','ECMWF-IFS-MR','FGOALS-f3-H','FGOALS-f3-L','FGOALS-g3','FIO-ESM-2-0','GFDL-AM4','GFDL-CM4','GFDL-CM4C192','GFDL-ESM2M','GFDL-ESM4','GFDL-GRTCODE','GFDL-OM4p5B','GFDL-RFM-DISORT','GISS-E2-1-G','GISS-E2-1-G-CC','GISS-E2-1-H','GISS-E2-2-G','GISS-E3-G','HadGEM3-GC31-HH','HadGEM3-GC31-HM','HadGEM3-GC31-LL','HadGEM3-GC31-LM','HadGEM3-GC31-MH','HadGEM3-GC31-MM','IITM-ESM','INM-CM4-8','INM-CM5-0','INM-CM5-H','IPSL-CM6A-ATM-HR','IPSL-CM6A-LR','KACE-1-0-G','LBLRTM-12-8','MCM-UA-1-0','MIROC-ES2H-NB','MIROC-ES2L','MIROC6','MPI-ESM-1-2-HAM','MPI-ESM1-2-HR','MPI-ESM1-2-LR','MPI-ESM1-2-XR','MRI-AGCM3-2-H','MRI-AGCM3-2-S','MRI-ESM2-0','NESM3','NICAM16-7S','NICAM16-8S','NICAM16-9S','NorCPM1','NorESM1-F','NorESM2-LM','NorESM2-MM','RRTMG-LW-4-91','RRTMG-SW-4-02','RTE-RRTMGP-181204','SAM0-UNICON','TaiESM1','UKESM1-0-LL']

# available frequencies
cmip_freq=['1hr','1hrCM','3hr','3hrPt','6hr','6hrPt','day','dec','fx','mon','monC','monPt','month','subhrPt','yr','yrPt']

def cmip6_search(models = "all",
        var = None,
        year_range = None,
        frequency = "mon",
        variant = "all",
        experiment = ["historical", "ssp119", "ssp126", "ssp245", "ssp370", "ssp434", "ssp460", "ssp585", "ssp534-over" ]):

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

    if models != "all":
        for model in models:
            if model not in cmip6_models():
                raise ValueError(f"{model} is not a valid CMIP6 model!")


    if frequency not in cmip_freq:
        raise ValueError(f"{model} is not a valid CMIP6 model!")

    tracker = 1
    files = []
    urls = []
    check_sums = []

    for url in url_list:
        print(f"Getting files from {url}")

        new_url = url + "/esg-search/wget?project=CMIP6"

        if models != "all":
            for model in models:
                new_url = f"{new_url}&source_id={model}"

        if var is not None:
            new_url = f"{new_url}&variable={var}"

        new_url = f"{new_url}&frequency={frequency}"
        for ee in experiment:
            new_url = f"{new_url}&experiment_id={ee}"

        if variant != "all":
            if type(variant) is str:
                variant = [variant]
            for vv in variant:
                new_url = f"{new_url}&variant_label={vv}"

        new_url =f"{new_url}&limit=10000"

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
                        urls.append(line.split(" ")[1].replace("'", ""))
                        check_sums.append(line.split(" ")[3])


    	# Now pull out the years

            if change_level == 1:
                if "download_files" in line:
                    change_level = 2
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






