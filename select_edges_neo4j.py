# Import py2Nneo library as Neo4J driver and other libs
import os
import json
from py2neo import Graph
from pprint import pprint
from datetime import datetime

# Set some  variables
_current_wd = os.getcwd()
_out_path = _current_wd + '/out'
_log_out_path = _out_path + '/running_output'
_program_output = _out_path + '/network_outputs'
_download_output = _out_path + '/download_outputs'
_jsonl_path = _program_output + '/jsonl'

_run_date = datetime.now().strftime("%Y%m%d")
_run_time = datetime.now().strftime("%H.%M")

# Create a graph object connecting to the URL
graph = Graph('bolt://neo4j.het.io:7687')

# Retrieve all the Gene to BP relations
result = graph.run(
    "MATCH (n:Gene), (bp:BiologicalProcess) MATCH p=(n)-[r:PARTICIPATES_GpBP]-(bp) RETURN n.identifier, bp.identifier, r"
)

# Format the data following HelioNEt edge data model
selected_edges = []
for record in result.data():
    mapping = {
            "source_id": [
                "Gene", record['n.identifier']
            ],
            "target_id": [
                "Molecular Function", record['bp.identifier']
            ],
            "kind": "participates",
            "direction": "both",
            "data": {
                record['r']
            }
        }
    selected_edges.append(mapping)

# Add date to the json output
json_file_name = '/neo4j_G-BP_output_{}.jsonl'.format(_run_date)

# Write the result into a jsonl file
with open(_program_output + json_file_name, 'w') as outfile:
    for edge in selected_edges:
        outfile.write(str(edge) + '\n')

#%%
outfile.close()
