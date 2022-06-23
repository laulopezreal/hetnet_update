# Import custom utils library
from tools.utils import *

# Define program global Variables
_current_wd = os.getcwd()
_out_path = _current_wd + '/out'
_log_out_path = _out_path + '/running_output'
_program_output = _out_path + '/network_outputs'
_download_output = _out_path + '/download_outputs'
_jsonl_path = _program_output + '/jsonl'

_run_date = datetime.now().strftime("%Y%m%d")
_run_time = datetime.now().strftime("%H.%M")
warnings.filterwarnings("ignore")

# Program starts here #

if __name__ == '__main__':
    """
    1) G-BP Extraction Pipeline
    """
    logger, oh, eh = logger_outputs(_log_out_path, _run_date, _run_time)
    logger.info('Environment set, program starts running. Current working directory is {}'.format(_current_wd))

    # Download HetioNetJSON
    download_url = 'https://github.com/hetio/hetionet/raw/master/hetnet/json/hetionet-v1.0.json.bz2'
    logger.info('Start download hetionet compressed json from its origin URL: {} using HTTP'.format(download_url))
    os.makedirs(_download_output, exist_ok=True)
    response = requests.get(download_url, stream=True)
    with open(_download_output + '/' + 'hetionet-v1.0.json.bz2', 'wb') as f:
        for data in response:
            f.write(data)
    # Sanity check
    while 'hetionet-v1.0.json.bz2' not in os.listdir(_download_output):
        logger.info('Waiting for download to complete and streaming writing.')

    # Read json.bz2 file in streaming mode and convert it to jsonl
    logger.info('DOWNLOAD COMPLETE: Read json.bz2 downloaded file in streaming mode and convert it to jsonl')
    with bz2.open(_download_output + '/hetionet-v1.0.json.bz2', 'rb') as f:
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
    N, E = convert_jsonl_to_bags(logger, _jsonl_path, print_example=True)

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

    # Convert to df and save the network as a jsonl compressed file
    logger.info('Converting the records of the extracted G-BP Edges into a Dask DataFrame '
                'and save it as a jsonl output file.')
    if not os.path.exists(_program_output):
        logging.debug('Creating new folder at {}'.format(_program_output))
        os.makedirs(_program_output)
    # define the absolute path
    df_jsonl_name = 'G-BP_edges_formated_{}.jsonl.gzip'.format(_run_date)
    output_absolute_path = _program_output + '/' + df_jsonl_name

    # execute the function
    hetionet_df = convert_to_dd(logger, selected_edges, print_head=True, save_jsonl=True,
                                jsonl_path=output_absolute_path)
    # Sanity Check
    try:
        df_jsonl_name in os.listdir(_program_output)
    except:
        FileNotFoundError('{} not found in {}. Wrong wrong during the export')
    logger.info('Export of {} complete at {}!'.format(df_jsonl_name, _program_output))

    # 2) Retrieve G-BP relations from http://api.geneontology.org/api
    logger.info('START UPDATE RELATIONS STAGE: retrieve G-BP relationships from the Gene Ontology (GO) API: {}'.format(
        '/http://api.geneontology.org/api'))

    # Define mapping function
    def maper(x):
        return {
            'type': x['type'],
            'subject': x['subject'],
            'object': x['object'],
            'relation': x['relation'],
            'evidence_types': x['evidence_types'],
            'provided_by': x['provided_by'],
        }
    # Restarting the Dask Client with a New configuration
    threads_per_worker = 2
    logger.info(
        'Initiating Dask Client with the next parameters: {} workers, {} threads x worker:, memory_limit of {} as the next task is more exhaustive.'.format(
            n_workers, threads_per_worker, memory_limit))
    client = initiate_dask_client(n_workers, threads_per_worker, memory_limit)

    # Sanity check
    try:
        client.status == 'running'
    except:
        raise EnvironmentError('Dask client could not initiate.')
    logger.info('Dask client status is {}. INFO: {}'.format(client.status, client))
    logger.info(
        'Call the GO API to obtain the updated list of Gene to Biological Process relations. Public available API at http://api.geneontology.org/api/')
    # Some variables
    _start = 1
    _end = 3367291
    _batch = 1000

    #  Calling the API in vatches
    edge_list = []
    logger.info(
        'Only G-BP for human genes need to be considered. Filter results in batches and store the corres relationships')
    for i in range(1, 3367291, 1000):
        url = 'http://api.geneontology.org/api/association/to/GO%3A0008150?rows=1000&start={}&unselect_evidence=true&exclude_automatic_assertions=false&use_compact_associations=false'.format(
            i)
        logger.debug('Batch of 1000 results n retrieved from the server {}'.format(i, url))
        get = db.read_text(url).map(json.loads).map(lambda x: x['associations']).flatten()
        match = get.map(maper).filter(lambda x: x['subject']['taxon']['id'] == 'NCBITaxon:9606')
        edge_list.append(match.compute())
        percentage = i / 3367291 * 100
        logger.info('Records checked up to {}%'.format(percentage))

    # Map the results to hetionet format
    logger.info('Mapping the results of the updated G-BP from GO API to HetioNet format.')
    GO_API_df_list = []
    for record in edge_list:
        data = {
            "source_id": [
                "Gene", record['source']['id']
            ],
            "target_id": [
                "Biological Process", record['object']['id']
            ],
            "kind": "participates",
            "direction": "both",
            "data": {
                "source": record['provided_by'],
                "license": "CC BY 4.0",
                "unbiased": "false"
            }
        }
        GO_API_df_list.append(data)

    # Convert to Pandas Data Frame
    logger.info('Convert to a Pandas Data Frame')
    GO_API_df = pd.DataFrame(GO_API_df_list)
    print(GO_API_df.describe())
    logger.info('Showing first rows of the DataFrame:\n----- G-BP Updated DataFrame')
    print(GO_API_df.head().iloc[:, 0:4])
    print('-----')

    # Show first lines of the HetioNet Data Frame
    logger.info('Showing first rows of the old HeloNet DataFrame:\n----- G-BP Old Hetionet DataFrame')
    print(hetionet_df.head().iloc[:, 0:4])
    print('-----')

    # Convert to df and save the network as a jsonl compressed file
    logger.info('Saving the records of the updated G-BP as a jsonl gzip compressed output file.')
    os.makedirs(_program_output, exist_ok=True)
    # define the absolute path
    new_df_jsonl_name = 'UPDATED_G-BP_edges_formated_{}.jsonl.gzip'.format(_run_date)
    output_absolute_path = _program_output + '/' + df_jsonl_name
    GO_API_df.to_json(output_absolute_path, index=False, compression='gzip')

    logger.info(
        'DONE! New jsonl file {} with the updated relations ready at {}.'.format(new_df_jsonl_name, _program_output))

    client.close()
    # Close the handlers
    oh.close()
    eh.close()
