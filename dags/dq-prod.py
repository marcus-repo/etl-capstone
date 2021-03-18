import sys
from datetime import datetime

############ local only #############################################
#####################################################################
# import configparser
import os

# # read configurations
# config = configparser.ConfigParser()
# config.read_file(open('aws.cfg'))

# # AWS stuff
# os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'KEY')
# os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'SECRET')

# KEY = config.get('AWS', 'KEY')
# SECRET = config.get('AWS', 'SECRET')
# REGION = config.get('AWS', 'REGION')

# findspark
# import findspark
# findspark.init()
# print(findspark.find())
# print(os.environ['SPARK_HOME'])
# print(os.environ['JAVA_HOME'])
# print(os.environ['HADOOP_HOME'])
####################################################################


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *


def create_spark_session(local=True):
    """
    Creates and returns spark session.
    """
    if local:
        spark = SparkSession \
            .builder \
            .enableHiveSupport() \
            .getOrCreate()
        
    else:
        spark = SparkSession \
            .builder \
            .enableHiveSupport() \
            .getOrCreate()
        
    return spark


def has_rows(year, month, path, df):
    """ Tests if a spark dataframe has rows. 
        Test fails if dataframe is not defined or row count equals 0.
        Test passes if dataframe row count is greater than 0.

    :params year: reference year of the dataset
    :params month: reference month of the dataset
    :params path: refrenence path (source) of the dataset
    :params df: spark dataframe
    :return d: tuple with test results
    """
    d = tuple()
    if df is None:
        d = (year, month, path, 'has_rows', 'failed', None, None)
    else:
        num_recs = df.count()
        if num_recs == 0:
            d = (year, month, path, 'has_rows', 'failed', None, num_recs)
        elif num_recs > 0:
            d = (year, month, path, 'has_rows', 'passed', None, num_recs)
        else:
            # should not happen but good practice
            d = (year, month, path, 'has_rows', 'failed', None, num_recs)
    return d


def has_nulls(year, month, path, df, fields=[]):
    """ Tests if specified fields of a spark dataframe has null values.
        Test fails if field null counts greater than 0.
        Test passes if field null counts equals 0.

    :params year: reference year of the dataset
    :params month: reference month of the dataset
    :params path: refrenence path (source) of the dataset
    :params df: spark dataframe
    :params fields: list of fieldnames in the dataframe to be tested
    :return l: list of tuples with test results
    """

    # list of all results
    l = []
    # temporary test result for single field
    d = tuple()
    # loop through fields and gather test results
    for field in fields:
        num_recs = df.select(field).where(col(field).isNull()).count()
        if num_recs > 0:
            d = (year, month, path, 'has_nulls', 'failed', field, num_recs)
        elif num_recs == 0:
            d = (year, month, path, 'has_nulls', 'passed', field, num_recs)
        else:
            # should not happen but good practice
            d = (year, month, path, 'has_nulls', 'failed', field, num_recs)
        # store test result in list
        l.append(d)

    return l


def execute_dq_test(spark, year, month, input_data):
    """ Executes has_rows and has_nulls test for multiple datasets/fields.
        Loops through file paths, reads data from path into spark dataframe.
        Calls has_rows and has_nulls functions.
        Gathers test results and returns these as list of tuples.

    :params spark: spark session
    :params year: reference year of the dataset
    :params month: reference month of the dataset
    :params input_data: dictionary with filepath to S3 parquet data, 
                        list of fieldnames used in has_nulls test
    :return results: list of tuples with test results of all datasets
    """

    # store test results
    has_rows_result = []
    has_nulls_result = []
    
    # loop through path, fields
    for path, fields in input_data.items():
        print(path)
        
        # read data from specified location
        try:
            df_spark = spark.read.parquet(path)  
        except:
            print("Error while reading the data.")
        else:
            # perform tests
            has_rows_result.append(has_rows(year, month, path, df_spark))
            has_nulls_result.append(has_nulls(year, month, path, df_spark, fields))
            
    # function to flatten list of lists
    # has_nulls_result is nested due to single/multiple field specifications
    flatten = lambda t: [item for sublist in t for item in sublist]
    
    # combine test results in a final results list
    results = has_rows_result + flatten(has_nulls_result)
    
    return results


def create_dq_report(spark, results, output_data, local):
    """ Creates a data quality report and stores it in parquet file format.

    :params spark: spark session
    :params results: list of tuples following the dq_schema
    :params output_data: location where report is stored
    :return failed_count: number of failed tests
    """
    
    # schema of the expected results input
    dq_schema = StructType([       
        StructField('year', StringType(), True),
        StructField('month', StringType(), True),
        StructField('path', StringType(), True),
        StructField('test', StringType(), True),
        StructField('result', StringType(), True),
        StructField('field', StringType(), True),
        StructField('num_recs', StringType(), True),
    ])
    
    # create dataframe from results and schema
    df_results = spark.createDataFrame(data=results, schema=dq_schema)
    
    # write out data quality report
    df_results = df_results.repartition(1)
    df_results.write.parquet(output_data+"data_quality.parquet", mode="append")
    
    if local:
        df_results.write.csv(output_data+"data_quality", mode="overwrite", header=True, sep=";")

    # count quality test failures
    failed_count = df_results.select("result").where(col("result")=="failed").count()
    return failed_count
    


def main():
    """ script performs data quality test after load to the final datamodel.
        - create spark session
        - read files from given S3 location
        - execute data quality tests: has_rows and has_nulls on given files / specified fields
        - create dq-report (spark dataframe): combine results and write to location on S3
        - raise Exception if tests failed

    :params sys.argv[1]: year reference year of the dataset
    :params Sys.argv[2]: month reference month of the dataset
    """
    
    # print start-time
    print(datetime.now())    
    
    # flag for local test modus
    local = False
    if local:
        
        year='2016'
        month='4'
        
        # inputs / outputs
        input_data = {"../model/i94.parquet/year=2016/month=4/":                                
                                ['i94_id',
                                 'cic_id',
                                 'cit_country_id', 
                                 'res_country_id', 
                                 'mode_id',
                                 'purpose_id',
                                 'visa_id',
                                 'state_id',
                                 'arrival_date',
                                 'gender'],
                      "../model/visa_categories.parquet":['visa_id'],
                      "../model/us_states.parquet":['state_id'],
                      "../model/countries.parquet":['country_id'],
                      "../model/temperature.parquet":['country_id','month'],
                      "../model/mode.parquet":['mode_id'],
                      "../model/purpose.parquet":['purpose_id'],
                      "../model/dates.parquet":['date']
                      }

        output_data = "../model/"
          
    else:
        # inputs via sys args
        bucket_name = sys.argv[1]
        year = sys.argv[2]
        month = sys.argv[3]

        # inputs / outputs
        input_data = {"s3a://{}/model/i94.parquet/year={}/month={}".format(bucket_name, year, month):
                            ['i94_id',
                             'cic_id',
                             'cit_country_id', 
                             'res_country_id', 
                             'mode_id',
                             'purpose_id',
                             'visa_id',
                             'state_id',
                             'arrival_date',
                             'gender'],
                      "s3a://{}/model/visa_categories.parquet".format(bucket_name):['visa_id'],
                      "s3a://{}/model/us_states.parquet".format(bucket_name):['state_id'],
                      "s3a://{}/model/countries.parquet".format(bucket_name):['country_id'],
                      "s3a://{}/model/temperature.parquet".format(bucket_name):['country_id','month'],
                      "s3a://{}/model/mode.parquet".format(bucket_name):['mode_id'],
                      "s3a://{}/model/purpose.parquet".format(bucket_name):['purpose_id'],
                      "s3a://{}/model/dates.parquet/year={}/month={}".format(bucket_name, year, month):['date']
                      }

        output_data = "s3a://{}/model/".format(bucket_name)
            
    
    # create spark session
    spark = create_spark_session(local)
    
    # execute data quality test on all inputs
    dq_results = execute_dq_test(spark, year, month, input_data)

    # create data quality report
    failed_count = create_dq_report(spark, dq_results, output_data, local)

    # raise an error if one of tests did not pass
    if failed_count > 0:
        raise Exception("Data quality test did not pass.")

    # print end-time
    print(datetime.now())
    
   
if __name__ == "__main__":
    main()