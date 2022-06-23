# HetioNet Update Project

## Overview

HetNets stands for Heterogeneous Networks, which are graphs with nodes and edges of different types.

[Hetionet v1.0](https://github.com/hetio/hetionet) is a HetNet encoding biological, biomedical and pharmacology data sources. 

## Environment setup

### Use conda
This is the recommended way of setting up the environment. 
See [Conda documentation](https://docs.conda.io/en/latest/) on how to [install](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html) conda in your computer.
```shell
conda env create --name hetionet_update --file=environments.yml
```

### Using pip
To install the dependencies required using pip:
```shell
pip install -r requirements.txt
```

## HetioNet_update Jupyter Notebook

[HetioNet_update.ipnb](HetioNet_update.ipynb)  is jupyter notebook that implements the code to update the G-BP relations from the original HerioNet network.
This can be very useful for testing, training and demos.

It provides an interactive dashboard to visualise the CPU and Disk usage whilst running the workflow.
Simply run this jupyter notebook in order to construct the knowledge graph form the outputs of the processing files mentioned above. 
This jupyter notebook produces a `json.gzip` file with all the G-BP relations.



----------
**NOTE**: The program was run on a Linux OS, running the Unix open source distribution Ubuntu 20.04 LTS(64 bits); 
in a machine with a processor of 11th Gen Intel® Core™ i7-1185G7 @ 3.00GHz × 8 and 16 GiB of RAM. 

