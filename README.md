# HetioNet Update Project

## Overview

HetNets stands for Heterogeneous Networks, which are graphs with nodes and edges of different types.

[Hetionet v1.0](https://github.com/hetio/hetionet) is a HetNet encoding biological, biomedical and pharmacology data sources.

### Aim of the project
 We are interested in updating the published knowledge graph, [HetioNet](https://het.io)
As the resource was established some time ago, some underlying resources might have been updated. 

The project focus on isolate the Human Genes to GO Biological Process relations (gene G participates in process BP) within HetioNet.
Then it uses the GO Rest API to (http://api.geneontology.org/api) to understand how these relations have changed to date and update the network.


## Environment setup

### env_setup.sh 
The root of the project contains a [env_setup.sh](env_setup.sh) bash script that install everything for you. The only requisite is that you have conda preinstalled in your computer!

See [Conda documentation](https://docs.conda.io/en/latest/) on how to [install](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html) conda in your local machine.
```shell
./env_setup.sh
```
### Use conda
This is the other recommended way of setting up the environment. 
```shell
conda env create --name hetionet_project --file=environment.yml
```

## HetioNet_update Jupyter Notebook

[HetioNet_update.ipnb](HetioNet_update.ipynb)  is jupyter notebook that implements the code to update the G-BP relations from the original HerioNet network.
This can be very useful for testing, training and demos.

Run the following Terminal command to install some packages in your base conda environment.
Run the `jupyter lab` command in your Terminal, and it should automatically open a browser window.
```shell
# Install plugins
conda install -c conda-forge dask jupyterlab dask-labextension s3fs

# Start jupyter lab server
jupyter lab
```
It provides an interactive dashboard to visualise the CPU and Disk usage whilst running the workflow.
Simply run this jupyter notebook in order to construct the knowledge graph form the outputs of the processing files mentioned above. 
This jupyter notebook produces a `json.gzip` file with all the G-BP relations.



----------
**NOTE**: The program was run on a Linux OS, running the Unix open source distribution Ubuntu 20.04 LTS(64 bits); 
in a machine with a processor of 11th Gen Intel® Core™ i7-1185G7 @ 3.00GHz × 8 and 16 GiB of RAM. 

