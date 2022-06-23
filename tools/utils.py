# Import required libraries
import os
import sys
import bz2
import json
import dask
import warnings
import logging
import requests
import argparse
import pandas as pd

from pprint import pprint
from datetime import datetime

import dask.bag as db
import dask.dataframe as dd
from dask.distributed import Client




# Define program functions
def logger_outputs(_log_out_path, _run_date, _run_time):
    """
    Function to create the loggers and directories where the output logs will be stored.
    :return: logger
    """
    # Create output dirs
    path_exists = os.path.exists(_log_out_path + '/' + _run_date)
    if path_exists == False:
        os.makedirs(_log_out_path + '/' + _run_date, exist_ok=True)
    out_name = 'helionet_run.{}.out'.format(_run_time)
    err_name = 'helionet_run.{}.err'.format(_run_time)

    # Create loggers
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # create stream handler which logs INFO messages
    console = logging.StreamHandler(stream=sys.stdout)
    console.setLevel(logging.INFO)
    # create file handler which logs INFO messages
    oh = logging.FileHandler(_log_out_path + '/' + _run_date + '/' + out_name)
    oh.setLevel(logging.DEBUG)
    # create file handler which logs ERROR messages
    eh = logging.FileHandler(_log_out_path + '/' + _run_date + '/' + err_name)
    eh.setLevel(logging.ERROR)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s: %(message)s')
    console.setFormatter(formatter)
    eh.setFormatter(formatter)
    oh.setFormatter(formatter)
    # add the handlers to logger
    logger.addHandler(eh)
    logger.addHandler(oh)
    logger.addHandler(console)
    return logger, oh, eh


def initiate_dask_client(n_workers, threads_per_worker, memory_limit):
    """
    Initiates the dask cluster with custom params
    :param n_workers: number of workers to use.
    :param threads_per_worker: number of threats per worker
    :param memory_limit: maximum memory.
    :return: client (Dask Client instance initiated)
    """
    client = Client(n_workers=n_workers, threads_per_worker=threads_per_worker, memory_limit=memory_limit)
    return client


def convert_jsonl_to_bags(logger, _jsonl_path, count=False, print_example=False):
    """
    Take jsonl records and converts them into Dask bags for parsing.
    :param count: Count number of records. Slows down performance. Default false.
    :param print_example: Print an example of a Node and Edge record
    :return: Node and Edge Dask bags.
    """
    N = db.read_text(_jsonl_path + '/*_nodes.jsonl').map(json.loads)
    E = db.read_text(_jsonl_path + '/*_edges.jsonl').map(json.loads)
    logger.info('Nodes and Edges bags created from {}.'.format(_jsonl_path))
    if count:
        logger.info('Number of records in Nodes: {}. Number of records in Edges: {}'.format(
            N.count().compute(), E.count().compute()))
    if print_example:
        logger.info('Example of Node Record:')
        print('-------')
        pprint(N.take(1))
        print('-------')
        logger.info('Example of Edge Record:')
        print('-------')
        pprint(E.take(1))
        print('-------')
    return N, E


def select_gene_BP_nodes(N, compute=False):
    """
    Select Gene and Biological Process Nodes from the Network
    :param N: Node records (Dask Bag)
    :param compute: If True computes the operation over the bag.Slows down performance. Default false.
    :return:
    """

    # Function to select Gene and Biological Process nodes
    def is_node_type(x):
        return x['kind'] == 'Gene' or x['kind'] == 'Biological Process'

    # Apply filter
    selected_nodes = N.filter(lambda record: is_node_type(record))
    if compute:
        computed_selected_nodes = selected_nodes.compute()
        return selected_nodes, computed_selected_nodes
    else:
        return selected_nodes


def select_gene_BP_edges(E, compute=False):
    """
    Select Gene to Biological Process Edges from the Network
    :param E: Edges records (Dask Bag).
    :param compute: If True computes the operation over the bag.Slows down performance. Default false.
    :return: selected_nodes: selected bag
    """

    # Function to select Gene to Biological Process relationships
    def select_edges(x, y):
        if x == 'Gene':
            boolean = (y == 'Biological Process')
        elif x == 'Biological Process':
            boolean = (y == 'Gene')
        else:
            boolean = False
        return (boolean)

    # Apply filter
    selected_edges = E.filter(lambda x: select_edges(x['source_id'][0], x['target_id'][0]))
    if compute:
        computed_selected_edges = selected_edges.compute()
        return selected_edges, computed_selected_edges
    else:
        return selected_edges


def convert_to_dd(logger, selected_edges, print_head=False, save_jsonl=False, jsonl_path=os.getcwd()):
    """
    Create Dask DataFrame from the selected edges records
    :param selected_edges:
    :param print_head: print the first rows of the generated Dask DataFrame
    :param save_json: save the Dask DataFrame as a jsonl file.
    :param save_json: path where jsonl is produced. Default is working dir.
    :return: df (Dask DataFrame)
    """

    # Function to flatten the data
    def flatten_data(record):
        return {
            'source_type': record['source_id'][0],
            'source_id': str(record['source_id'][1]),
            'target_type': record['target_id'][0],
            'target_id': record['target_id'][1],
            'kind': record['kind'],
            'direction': record['direction'],
            'data': record['data']
        }

    # Create Dask DataFrame to store the selected GENE to BIOLOGICAL PROCESS edged into a json file
    df = selected_edges.map(flatten_data).to_dataframe()
    logger.info('Selected G-BP Edges Dask DataFrane successfully created.')
    if print_head:
        logger.info('Showing first rows of the DataFrame:\n----- G-BP Edges DataFrame')
        print(df.describe())
        print(df.head().iloc[:,0:4])
    if save_jsonl:
        df.to_json(jsonl_path, compression='gzip')
    return df