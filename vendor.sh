#!/bin/bash

# Ensure the script stops on errors
set -e

# Check for correct usage
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <crate-name> <crate-version>"
    exit 1
fi

# Read arguments
crate_name="$1"
crate_version="$2"

# Determine the directory structure based on the crate name length
name_length=${#crate_name}
if [ "$name_length" -gt 3 ]; then
    dir="index/${crate_name:0:2}/${crate_name:2:2}"
    url_dir="index.crates.io/${crate_name:0:2}/${crate_name:2:2}"
elif [ "$name_length" -eq 3 ]; then
    dir="index/3/${crate_name:0:1}"
    url_dir="index.crates.io/3/${crate_name:0:1}"
elif [ "$name_length" -eq 2 ]; then
    dir="index/2"
    url_dir="index.crates.io/2"
else
    echo "Error: Crate name too short"
    exit 1
fi

# Create the directory
mkdir -p "$dir"

# Define the JSON metadata URL and file path
json_url="https://${url_dir}/${crate_name}"
json_file="$dir/${crate_name}"

# Fetch and append JSON metadata to the file
echo "Fetching JSON metadata from $json_url"
curl -s "$json_url" >> "$json_file"
echo "" >> "$json_file"  # Add a newline for clarity

# Define the crate download URL
crate_download_url="https://crates.io/api/v1/crates/${crate_name}/${crate_version}/download"
crate_file="${crate_name}-${crate_version}.crate"

# Check if the crate is already downloaded
if [ -f "$crate_file" ]; then
    echo "Crate $crate_file already exists. Skipping download."
else
    # Download the crate to the current directory
    echo "Downloading crate from $crate_download_url"
    aria2c -o "$crate_file" "$crate_download_url"
    echo "Crate $crate_file downloaded successfully!"
fi

