#!/bin/bash
# check to see if pipenv is installed

echo '... running script on requirment.txt'
rm -rf packages*
# install packages to a temporary directory and zip it
touch requirements.txt  # safeguard in case there are no packages
pip3 install -r requirements.txt
echo '... adding all modules from local utils package'
zip -ru9 packages.zip dependencies -x dependencies/__pycache__/\*

zip -ru9 packages.zip configs -x configs/__pycache__/\*