# Hellofresh Spark Challenge

This project is designed to be read in parallel from local file system in pyspark with code `HelloFresh_Challenge`. This project addresses the following topics:

- Reading JSON data;
- Transformations against given Data;
- Load data to output file;
- Test case to check transformation and extractor;
#### Just something I want to mention I diagnosed COVID positive on Friday evening but I still managed to complete the test as i promised with recruiter. Due to bad health it took more time to complete the test otherwise I can complete this in 5-6 hours
### Libraries Used

- pyspark
- configparser
- pytest

### Required Installation on OS

- Python 3.7
- spark 3.1.1

## ETL Project Structure

The basic project structure is as follows:

```bash
root/
 |-- configs/
 |   |-- config.ini
 |-- dependencies/
 |   |-- extractor.py
 |   |-- loader.py
 |   |-- logging.py
 |   |-- spark.py
 |   |-- transformer.py
 |-- jobs/
 |   |-- etl_job.py
 |-- tests/
 |   |-- test_etl_job.py
 |   build_dependencies.sh
 |   packages.zip
 |   requirements.txt
```
This project is designed to run ETL using Spark to achieve parallelization.
The main Python module containing the ETL job (which will be sent to the Spark cluster), is `jobs/etl_job.py`. Any external 
configuration parameters required by `etl_job.py` are stored in JSON format in `configs/etl_config.json`. Additional modules 
that support this job can be kept in the `dependencies` folder . In the project's root i include `build_dependencies.sh`, which 
is a bash script for building these dependencies into a zip-file to be sent to the cluster (`packages.zip`). 
Unit test modules are kept in the `tests`.

## Installing this Projects' Dependencies

Make sure that you're in the project's root directory (the same one in which the `requirements.txt` resides), and then run,

```bash
./build_dependencies.sh
```

## Structure of an ETL Job

In order to facilitate easy debugging and testing, i have isolated job in Extract,Load and Transform classes.

## Packaging ETL Job Dependencies

In this project i have used different functions that can be used across different ETL jobs are kept in a module called `dependencies` and referenced in specific job modules using, for example,

``` python
from dependencies.spark import start_spark
```

To make this task easier, especially when modules such as `dependencies` have additional dependencies (e.g. the `pyspark` package), i have created `build_dependencies.sh` bash script for automating the production of `packages.zip`.
## Running the ETL job

To run a ETL job first we need to run build_dependencies.sh to clean ETL package.
` ./build_dependencies.sh `
`$SPARK_HOME` environment variable points to my local Spark installation folder, then the ETL job can be run from the project's root directory using the following 
 
```bash
$SPARK_HOME/bin/spark-submit \
--master local[*],spark://99863b29b2a8:7077 \
--py-files packages.zip \
jobs/etl_job.py
```


Briefly, the options supplied serve the following purposes:

- `--master local[*]` - the address of the Spark cluster to start the job on. If you have a Spark cluster in operation (either in single-executor mode locally, or something larger in the cloud) and want to send the job there, then modify this with the appropriate Spark IP - e.g. `spark://the-clusters-ip-address:7077`;
- `--jars` -  JDBC driver for connecting to a relational database
- `--py-files packages.zip` - archive containing Python dependencies (modules) referenced by the job; and,
- `jobs/etl_job.py` - the Python module file containing the ETL job to execute.
## Automated Testing

In order to test with Spark, i use the `pyspark` Python package, which is bundled with the Spark JARs required to programmatically start-up and tear-down a local Spark instance, on a per-test-suite basis (we recommend using the `setUp` and `tearDown` methods in `unittest.TestCase` to do this once per test-suite). Note, that using `pyspark` to run Spark is an alternative way of developing with Spark as opposed to using the PySpark shell or `spark-submit`.
I have not defined number of executor and   cores of executors because currently we don't have to much data.
Given that i have chosen to structure our ETL jobs in such a way as to isolate the 'Load and Extract' steps.

To execute the unit test run,

```bash
python3 -m unittest tests/test_*.py
```
