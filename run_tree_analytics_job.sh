#!/usr/bin/env bash
if [[ $1 == "help" ]]
	then
		echo "Usage: run_tree_analytics_job.sh [spark_cluster_url], job will run on local standalone cluster if no spark url is provided"
        exit 0
fi

if [ $# -eq 0 ]
  then
    spark_url="local[*]"
  else
    spark_url=$1
fi
echo $spark_url

python3.7 -m venv env
chmod -R 777 ./env
source ./env/bin/activate
export PYSPARK_PYTHON=python3.7

which python
export PIPENV_VERBOSITY=-1
pip install -r requirements.txt

zip -ru9 packages.zip dependencies -x dependencies/__pycache__/\*

echo $SPARK_HOME

source $SPARK_HOME/bin/spark-submit \
--master $spark_url \
--py-files packages.zip \
--files configs/etl_config.json \
jobs/etl_job.py

source deactivate