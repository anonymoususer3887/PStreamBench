#!/usr/bin/env bash
# Downloads the real Word Count and Ad Analytics datasets from Kaggle into
# PStreamBench_local_docker/data/ (the upstream download_datasets.sh in
# PStreamBench_Cloud_setup hardcodes /home/playground/ and all 14 workloads'
# datasets; this is the two-dataset subset needed for this smoke test, kept
# local to this repo folder instead).
#
# Requires Kaggle API credentials. By default this reuses the project's
# existing kaggle.json (ask before pointing KAGGLE_CONFIG_DIR elsewhere if
# you don't have access to that file -- see README.md).
set -euo pipefail
cd "$(dirname "$0")/.."

KAGGLE_CONFIG_DIR="${KAGGLE_CONFIG_DIR:-$HOME/.kaggle}"   # folder with your own kaggle.json
export KAGGLE_CONFIG_DIR

mkdir -p data/zip
cd data/zip

echo "+ downloading wordcount dataset"
../../venv/bin/kaggle datasets download -d shubhamsumalya/wordcount
echo "+ downloading adanalytics dataset"
../../venv/bin/kaggle datasets download -d shubhamsumalya/adanalytics

unzip -o -q wordcount.zip
unzip -o -q adanalytics.zip

tar -xf word-count.tar.xz -C ..
tar -xf adAnalytics.tar.xz -C ..

cd ..
rm -rf zip
echo
echo "+ done. data/ now has:"
ls -la word-count.txt adAnalytics.dat
