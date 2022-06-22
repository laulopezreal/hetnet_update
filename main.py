#!/usr/bin/env python3

# Import required libraries
import os
import sys
import json
import dask
import warnings

import logging
# import requests
import argparse
from pprint import pprint

import dask.bag as db
import dask.dataframe as dd
from dask.distributed import Client

from datetime import datetime

# Define program global Variables
_current_wd = os.getcwd()
_out_path = _current_wd + '/out'
_log_out_path = _out_path + '/running_output'
_program_output = _out_path + '/network_outputs'
_jsonl_path = _program_output + '/jsonl'

_run_date = datetime.now().strftime("%Y%m%d")
_run_time = datetime.now().strftime("%H.%M")
warnings.filterwarnings("ignore")


# Define program functions
def logger_outputs():
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


def convert_jsonl_to_bags(count=False, print_example=False):
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


def convert_to_dd(selected_edges, print_head=False, save_jsonl=False, jsonl_path=_current_wd):
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


"""  
Program starts here 
 """
if __name__ == '__main__':
    # 1) Extraction Pipeline

    logger, oh, eh = logger_outputs()
    logger.info('Environment set, program starts running. Current working directory is {}'.format(_current_wd))

    # HOW CAN I DOWNLOAD THE JSON? check later

    # Read json file in streaming mode and convert it to jsonl
    logging.info(
        'Reading network nested json file and start conversion into new line delimited json (see https://jsonlines.org/).')
    with open(_current_wd + '/test/data/hetionet-v1.0.json', 'rb') as f:
        json_data = json.load(f)
        keys = [data for data in json_data]
        for key in keys:
            if not os.path.exists(_jsonl_path):
                logging.debug('Creating new folder at {}'.format(_jsonl_path))
                os.makedirs(_jsonl_path)
            logging.debug('Generating jsonl: output_{}.jsonl.'.format(key))
            with open(_jsonl_path + '/output_{}.jsonl'.format(key), 'w') as outfile:
                to_write = json_data[key]
                for element in to_write:
                    outfile.write(json.dumps(element) + "\n")
    # Sanity Check
    logger.info('Conversion complete. Performing sanity check before starting the Extraction Pipeline')
    try:
        len(os.listdir(_jsonl_path)) == 5
    except:
        raise BlockingIOError('Not all jsonl files needed for downstream Dask pipeline found. Path {} contains: {}. '
                              'Try to rerun the program'.format(_jsonl_path, os.listdir(_jsonl_path).join(',')))

    logger.info('All jsonl files needed for the downstream Dask pipeline have been generated at {}:{}.'.format(
        _jsonl_path, str(os.listdir(_jsonl_path))))

    # Edge extraction starts
    logger.info('"Genes" to "Biological Process" Edges (G-BP) Extraction Pipeline begins.')
    # Start the Dask Client
    n_workers = 4
    threads_per_worker = 1
    memory_limit = '8GB'
    logger.info(
        'Initiating Dask Client with the next parameters: {} workers, {} threads x worker:, memory_limit of {}.'.format(
            n_workers, threads_per_worker, memory_limit))
    client = initiate_dask_client(n_workers, threads_per_worker, memory_limit)

    # Sanity check
    try:
        client.status == 'running'
    except:
        raise EnvironmentError('Dask client could not initiate.')
    logger.info('Dask client status is {}. INFO: {}'.format(client.status, client))

    # Convert jsonl files into a dask bag (ONLY NODE EDGES?, Remember there are other two)
    N, E = convert_jsonl_to_bags(print_example=True)

    # Number of Nodes of each type in the network
    logger.info('Computing the number of nodes of each type in the network')
    n_nodes = dict(N.map(lambda record: record['kind']).frequencies(sort=True).compute())
    logger.info('Number of nodes in the network: \n----- NODE TYPES')
    pprint(n_nodes)
    print('-----')

    # Extract only Gene and Biological Process Nodes
    logger.info('Extracting nodes of kind:"Gene" and kind:"Biological Process" from the network')
    selected_nodes = select_gene_BP_nodes(N)
    n_selected_nodes = dict(selected_nodes.map(lambda record: record['kind']).frequencies(sort=True).compute())
    logger.info('Nodes extracted, new records contain: \n----- SELECTED NETWORK NODE TYPES')
    pprint(n_selected_nodes)
    print('-----')

    # Filter to select Gene to Biological Process edges
    logger.info('Extracting G-BP Edges from the network')
    selected_edges = select_gene_BP_edges(E)
    n_selected_edges = dict(selected_edges.map(lambda record: record['kind']).frequencies(sort=True).compute())
    double = selected_edges.map(lambda record: record['target_id']).take(10), selected_edges.map(
        lambda record: record['source_id']).take(10)
    logger.info('G-BP Edges extracted successfully! New records contain: \n----- SELECTED EDGES')
    print('Type of edge and frequency of each type:')
    pprint(n_selected_edges)
    print('---\nSample of 20 Edges:')
    for i in range(0, len(double[0])):
        print(double[0][i], '------', list(n_selected_edges.keys()), '------', double[1][i])
    print('-----')

    # Convert to df
    logger.info('Converting the records of the extracted G-BP Edges into a Dask DataFrame '
                'and save it as a jsonl output file.')
    if not os.path.exists(_program_output):
        logging.debug('Creating new folder at {}'.format(_program_output))
        os.makedirs(_program_output)
    # define the absolute path
    df_jsonl_name = 'G-BP_edges_formated_{}.jsonl.gzip'.format(_run_date)
    output_absolute_path = _program_output + '/' + df_jsonl_name

    # execute the function
    convert_to_dd(selected_edges, print_head=True, save_jsonl=True, jsonl_path=output_absolute_path)
    # Sanity Check
    try:
        df_jsonl_name in os.listdir(_program_output)
    except:
        FileNotFoundError('{} not found in {}. Something must have gone wrong during the export')
    logger.info('Export of {} complete at {}!'.format(df_jsonl_name, _program_output))
    oh.close()
    eh.close()
