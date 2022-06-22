# %% Import required libraries
import os
import sys
import json
import dask
import logging
# import requests
import argparse
import dask.bag as db
import dask.dataframe as dd

#%% Create a dask dashboard to visualize the partitions -> it crashes
# from dask.distributed import Client
# client = Client()  # set up local cluster on your laptop
# client


# %% Define logger handlers (one file for logs and one for errors)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# create file handler which logs INFO messages
oh = logging.FileHandler('out/helionet_run.out')
oh.setLevel(logging.DEBUG)
# create file handler which logs ERROR messages
eh = logging.FileHandler('out/helionet_run.err')
eh.setLevel(logging.ERROR)
# create stream handler which logs INFO messages
console = logging.StreamHandler(stream=sys.stdout)
console.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s: %(message)s')
eh.setFormatter(formatter)
oh.setFormatter(formatter)
console.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(eh)
logger.addHandler(oh)
logger.addHandler(console)


#%%
def create_download_dir(temp=False):
    """
    Create a folder in current dir where the network json will be downloaded
    :param temp: if true, delete the folder after loading the json
    """
    current_wd = os.getcwd()
    logger.info('current dir is: {}'.format(current_wd))
    download_path = current_wd + '/network_files'
    pathExists = os.path.isdir(download_path)
    if pathExists:
        logger.info('Path {} already exists. Files will be downloaded here.'.format(download_path))
    else:
        os.mkdir(download_path)
        logger.info('Creating downloading path at: {}'.format(download_path))
    if temp:
        logger.info('Removing {} dir.'.format(download_path))
        os.rmdir(download_path)
        try:
            if not pathExists:
                logger.info('Temp dir {} with network files removed'.format(download_path))
        except FileExistsError as fe:
            logger.exception("{}. Temp dir {} could not be removed. ".format(fe, download_path))
            sys.exit(0)

    # %% HOW CAN I DOWNLOAD THE JSON? check later
    url = 'https://github.com/hetio/hetionet/blob/master/hetnet/json/hetionet-v1.0.json.bz2?raw=true'
    # Probable solution: using the terminal and git LFS --> https://packagecloud.io/github/git-lfs/install#bash-python
    # r = requests.get(url) THIS IS NOT EFFICIENT
    # r.headers['Content-Type']

    # HOW TO PARSE A BIG (747 MB) JSON OBJECT??? Dask library for python
    # https: // examples.dask.org / bag.html
    # This library even allows to parse the json data on the fly (reducing the memory usage even more)

#%%
json_network = db.read_text('/home/llopez/Desktop/metagraph.json')

#%%
import json
import ast
js = json_network.map(json.loads)
# js = json_network.map(ast.literal_eval())
# take: inspect first few elements
js.take(3)

#%%
import ast

parsed_json = ast.literal_eval(json_network)
#%%
json_network = json_network.str.replace("\n'", "\"")
json_network.take(1)
#%%
type(json_network)

#%%
records = json_network.map(json.loads)
#%%
type(records)

#%%

json_network = dd.read_json("https://github.com/hetio/hetionet/blob/master/hetnet/json/hetionet-v1.0-metagraph.json")




#%%
##### Main Script #########
if __name__ == '__main__':
    create_download_dir(temp=True)
    # close loggers

    oh.close()
    eh.close()

