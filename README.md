# San Francisco: Tree Distributions Project

This project is aimed to build an ETL pipeline to analyse and find the distribution of various types of trees 
planted in San Francisco area.

The different statistics this project will find are, 
- Which is the area that is covered with the most number of trees ?
- Which are the top 5 most common street trees in San Francisco?
- How many "Cherry Plum" trees are "DPW Maintained" ?
- How many "Banyan Fig" trees have a Permit Number ?

## Implemetation technology
It is decided to use `Pyspark` framework for the ETL/analytics job implementation.
### Version used for base frameworks
- Apache Spark 2.4.4
- Python 3.7

## ETL Project Structure

The basic project structure is as follows:

```bash
root/
 |-- configs/
 |   |-- etl_config.json
 |-- dependencies/
 |   |-- logging.py
 |   |-- spark.py
 |-- jobs/
 |   |-- etl_job.py
 |-- output-files
 |-- tests/
 |   |-- test_data/
 |   |-- | -- input-data/
 |   |-- | -- validation-data/
 |   |-- test_etl_job.py
 |   build_dependencies.sh
 |   LICENSE
 |   packages.zip
 |   Pipfile
 |   Pipfile.lock
 |   README.md
```

The main Python module containing the Pyspark job, is `jobs/etl_job.py`. Any external configuration parameters required by `etl_job.py` are stored in JSON format in `configs/etl_config.json`. The  `build_dependencies.sh` script, which is a bash script for building these dependencies into a zip-file to be sent to the cluster (`packages.zip`). Unit test modules are kept in the `tests` folder and small chunks of representative input and output data, to be used with the tests, are kept in `tests/test-data` directory.

## Structure of PySpark Job

As a general rule of thumb, the data manipulation functions should be designed to be __idempotent__ . This is a technical way of saying that the repeated application of the transformation function should have no impact on the fundamental state of output data, until the moment the input data changes. One of the key advantages of idempotent jobs, is that they can be set to run repeatedly by using `cron` to trigger the `spark-submit` on a pre-defined schedule, rather than having to factor-in potential dependencies on other jobs completing successfully.

## Passing Configuration Parameters to the ETL Job

The effective solution is to send Spark a separate file - e.g. using the `--files configs/etl_config.json` flag with `spark-submit`  

## Packaging ETL Job Dependencies

In this project, functions that can be used across different jobs are kept in a module called `dependencies` and referenced in specific job modules using, for example,

```python
from dependencies.spark import start_spark
```

This package, together with any additional dependencies referenced within it, must be copied to each Spark node for all jobs that use `dependencies` to run. This can be achieved in one of several ways:

1. send all dependencies as a `zip` archive together with the job, using `--py-files` with Spark submit;

Run the `build_dependencies.sh` bash script for automating the production of `packages.zip`, given a list of dependencies documented in `Pipfile`.
```bash
./build_dependencies.sh ./packages
```

## Running the ETL job

Asuuming correct `python` version is saved in `PYSPARK_PYTHON` variable, else follow,
```bash
pipenv install --dev

export PYSPARK_PYTHON=python3.7
```
Assuming that the `$SPARK_HOME` environment variable points to your local Spark installation folder, then the job can be run from the project's root directory using the following command from the terminal,

To submit the spark job follow the command,
```bash
$SPARK_HOME/bin/spark-submit \
--master local[*] \
--py-files packages.zip \
--files configs/etl_config.json \
jobs/etl_job.py
```

The options supplied as command line arguments serve the following purposes:

- `--master local[*]` - the address of the Spark cluster to start the job on. If you have a Spark cluster in operation and want to send the job there, then modify this with the appropriate Spark IP - e.g. `spark://the-clusters-ip-address:7077`;
- `--files configs/etl_config.json` - the (optional) path to any config file that may be required by the ETL job;
- `--py-files packages.zip` - archive containing Python dependencies (modules) referenced by the job; and,
- `jobs/etl_job.py` - the Python module file containing the ETL job to execute.


## Automated Testing

In order to test with Spark, we use the `pyspark` Python package, which is bundled with the Spark JARs required to programmatically start-up and tear-down a local Spark instance, on a per-test-suite basis using the `setUp` and `tearDown` methods in `unittest.TestCase` to do this once per test-suite. 
Given that the structure of jobs in such a way as, we are free to feed it a small slice of 'real-world' production data that has been persisted locally in `tests/test-data` or other easily accessible network directory - and check it against known results.

To execute the example unit test for this project run,

```bash
python -m unittest tests/test_*.py
```


# Expected output
## The out put files for the this assignment is checked in to the directory `output-files` in project root.
## All the output files will be saved in the configured output directory location in `configs/etl_config.json`. It can be accessed by
```bash
:>> ls -l <path/to/output/direcory> 
```
### Inline display of output data
### Which is the area that is covered with the most number of trees
address | 
--- | 
700 Junipero Serra Blvd |

### Which are the top 5 most common street trees in San Francisco?
tree_type|count
--- | --- |
Sycamore: London Plane|11383
New Zealand Xmas Tree|8677
Brisbane Box|8236
Victorian Box|6922
Swamp Myrtle|6781

### How many "Cherry Plum" trees are "DPW Maintained" ?
CherryPlumTrees | 
--- | 
121|

### How many "Banyan Fig" trees have a Permit Number ?
BanyanTreeCount | 
--- | 
144|

