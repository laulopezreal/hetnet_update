#!/bin/bash

## This script prepares de environment to run the HetioNet update project
set -e -o pipefail
conda env create --name hetionet_project --file=environments.yml
conda activate hetionet_project

## You might want to delete the env after running the project
## Remove environment and its dependencies
conda remove --name envname --all
#
## Other useful commands
## list all the conda environment available
#conda info --envs
## Create new environment named as `envname`
#conda create --name envname
### Clone an existing environment
#conda create --name clone_envname --clone envname