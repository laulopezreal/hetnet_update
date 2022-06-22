#!/bin/bash

## This script prepares de environment to run the data-load app.

# -e flag causes bash to exit at any point if there is any error,
# the -o pipefail flag tells bash to throw an error if it encounters an error within a pipeline,
# the -x flag causes bash to output each line as it is executed -- useful for debugging
set -e -o pipefail

# Source progress bar from tools
source ./tools/progress_bar.sh

# Check curl
curl_check ()
{
  echo "Checking for curl..."
  if command -v curl > /dev/null; then
    echo "Detected curl..."
  else
    echo "curl could not be found, please install curl and try again"
    exit 1
  fi
}

## Check python and pip

# Check jq


# Check if venv is activated
venv_check ()
{
  if [[ "$VIRTUAL_ENV" != "" ]]
  then
    echo "Virtual environment $VIRTUAL_ENV is activated"
    # Check if the environment has all the needed libraries
    echo "Install all the required python libraries"
    pip install -q -r requirements.txt
  else
    echo "ERROR: not able to activate virtual environment"
  fi
}

# Create download dir
mkdir download | cd download

# Download the json file
wget "https://github.com/hetio/hetionet/blob/master/hetnet/json/hetionet-v1.0.json.bz2"

# Unpack
tar -tvf hetionet-v1.0.json.bz2

# Convert to line delimited json so can be processed y dask

jq -c ".[]" hetionet-v1.json > network.jsonl


# progress bar https://github.com/pollev/bash_progress_bar
main() {
    # Make sure that the progress bar is cleaned up when user presses ctrl+c
    enable_trapping
    # Create progress bar
    setup_scroll_area
    for i in {1..99}
    do
        if [ $i = 50 ]; then
            echo "waiting for user input"
            block_progress_bar $i
            read -p "User input: "
        else
            jq -c ".[]" hetionet-v1.json > network.jsonl
            draw_progress_bar $i
        fi
    done
    destroy_scroll_area
}

main



## Create python env
python3 -m venv project_venv

## Activate python env
source project_venv/bin/activate

## Deactivate the venv --> This will go at the end of the main script
# deactivate
#
# # Check you have deactivated the VIRTUAL_ENV
# if [[ "$VIRTUAL_ENV" == "" ]]
#  then
#    echo "venv is deactivated"
# fi