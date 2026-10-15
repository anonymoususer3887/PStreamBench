#!/usr/bin/env bash
# Templated by infra/views.py::_spark_config_manipulation() (placeholders
# below are substituted before this file is copied to the cluster), mirroring
# how sample-flink-conf.yaml / flink-conf.yaml are handled for Flink.

# Master UI defaults to 8080, which collides with Storm's UI port (see
# PStreamBench_Cloud_setup/README.md ports table) -- move it next to
# Flink's dashboard port (8086) instead.
export SPARK_MASTER_WEBUI_PORT=8087
export SPARK_MASTER_HOST=ms0932

export SPARK_WORKER_CORES=16
export SPARK_LOCAL_IP=ms0945
