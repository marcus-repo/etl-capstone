import sys
from datetime import datetime, timedelta

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
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, coalesce, lit, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, weekofyear, datediff
from pyspark.sql.types import *


### helper functions
def convert_sas_date(x):
    """Converts sas integer date to date

    :params x: sas date type
    :return: date
    """
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None


def convert_str_to_date(x):
    """ Converts date-string to date

    :params x: date string Y-m-d
    :return: date
    """
    try:
        return datetime.strptime(x, "%Y%m%d")
    except:
        return None


def clean_negative_age(x):
    """ Sets negative age to null 

    :params x: age
    :return: Null if age < 0 else age
    """
    if x == None:
        return None
    elif x < 0:
        return None
    else:
        return x

def clean_gender(x):
    """ Cleans invalid gender information.
    :params: x
    :return: U (Unknow if not M or F)
    """
    if x in ['M','F']:
        return x
    else:
        return 'U'

### spark session
def create_spark_session(local=True):
    """
    Creates and returns spark session.
    """
    if local:
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages","saurfang:spark-sas7bdat:3.0.0-s_2.12") \
            .enableHiveSupport() \
            .getOrCreate()
        
    else:
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
            .enableHiveSupport() \
            .getOrCreate()
        
    return spark


def get_i94_data(spark, input_data):
    """ Reads i94 immigration data

    :params spark: spark session
    :params input data: filepath to SAS
    :return df_spark: spark dataframe
    """

    # schema definition
    i94_schema = StructType([
        StructField("cicid", IntegerType(), True),    # id
        StructField("i94yr", IntegerType(), True),    # Year
        StructField("i94mon", IntegerType(), True),   # Month
        StructField("i94cit", IntegerType(), True),   # Country Codes I94CIT represents the country of citizenship.
        StructField("i94res", IntegerType(), True),   # Country Codes I94RES represents the country of residence.
        StructField("i94port", StringType(), True),   # e. g. 'DTH' = 'DUTCH HARBOR, AK  
        StructField("arrdate", IntegerType(), True),  # ARRDATE is the Arrival Date in the USA. SAS date numeric field
        StructField("i94mode", IntegerType(), True),  # Air, Sea, Land ...
        StructField("i94addr", StringType(), True),   # States: FL, ...
        StructField("depdate", IntegerType(), True),  # SAS date numeric field 
        StructField("i94bir", IntegerType(), True),   # Age of Respondent in Years
        StructField("i94visa", IntegerType(), True),  # Business, Pleasure, Student
        StructField("count", IntegerType(), True),    # COUNT - Used for summary statistics
        StructField("dtadfile", StringType(), True),  # DTADFILE - Character Date Field - Date added to I-94 Files - CIC does not use
        StructField("visapost", StringType(), True),  # VISAPOST - Department of State where where Visa was issued - CIC does not use
        StructField("occup", StringType(), True),     # OCCUP - Occupation that will be performed in U.S. - CIC does not use
        StructField("entdepa", StringType(), True),   # ENTDEPA - Arrival Flag - admitted or paroled into the U.S. - CIC does not use
        StructField("entdepd", StringType(), True),   # ENTDEPD - Departure Flag - Departed, lost I-94 or is deceased - CIC does not use
        StructField("entdepu", StringType(), True),   # ENTDEPU - Update Flag - Either apprehended, overstayed, adjusted to perm residence - CIC does not use
        StructField("matflag", StringType(), True),   # MATFLAG - Match flag - Match of arrival and departure records
        StructField("biryear", IntegerType(), True),  # BIRYEAR - 4 digit year of birth
        StructField("dtaddto", StringType(), True),   # DTADDTO - Character Date Field - Date to which admitted to U.S. (allowed to stay until) - CIC does not use
        StructField("gender", StringType(), True),    # GENDER - Non-immigrant sex
        StructField("insnum", StringType(), True),    # INSNUM - INS number
        StructField("airline", StringType(), True),   # AIRLINE - Airline used to arrive in U.S.
        StructField("admnum", DoubleType(), True),    # ADMNUM - Admission Number
        StructField("fltno", StringType(), True),     # FLTNO - Flight number of Airline used to arrive in U.S.
        StructField("visatype", StringType(), True),  # VISATYPE - Class of admission legally admitting the non-immigrant to temporarily stay in U.S.
    ])     
    
    # read sas datafile
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load(input_data, schema=i94_schema)
    #print(df_spark.count())
    
    return df_spark


def get_country_mapping(spark, input_data):
    """ Reads country mapping file.

    :params spark: spark session
    :params input_data: filepath csv
    :return: spark dataframe
    """
    
    country_map_schema = StructType([
        StructField("i94_code", IntegerType(), False),
        StructField("i94_desc", StringType(), False),
        StructField("iso_code", StringType(), False),
    ])   

    df_con = spark.read.csv(input_data, sep=";", header=True, schema=country_map_schema)
    return df_con
    

def get_visa_mapping(spark, input_data):
    """ Reads visa category mapping file.

    :params spark: spark session
    :params input_data: filepath csv
    :return: spark dataframe
    """
    
    visa_cat_schema = StructType([
        StructField("visa_category", StringType(), False),
        StructField("visa_group", StringType(), False),
        StructField("visa_desc", StringType(), False),
        StructField("visa", StringType(), True),
        StructField("visa_id", IntegerType(), False),
    ])      
    
    df_visa = spark.read.csv(input_data, sep=";", header=True, schema=visa_cat_schema)
    return df_visa


def get_us_states_mapping(spark, input_data):
    """ Reads us states mapping file.

    :params spark: spark session
    :params input_data: filepath csv
    :return: spark dataframe
    """
    
    us_states_schema = StructType([
        StructField("state_id", StringType(), False),
        StructField("state_name", StringType(), False),
    ])
    
    df_states = spark.read.csv(input_data, sep=";", header=True, schema=us_states_schema)
    return df_states


def process_country_data(spark, input_data, output_data, local):
    """ Reads country / continent dimesion table
        Writes data to output location in parquet file format

    :params spark: spark session
    :params input_data: filepath csv
    :params output: filepath
    """
    
    country_schema = StructType([
        StructField("country_id", StringType(), False),
        StructField("country_name", StringType(), False),
        StructField("continent_name", StringType(), False),
    ])   

    df_con = spark.read.csv(input_data, sep=";", header=True, schema=country_schema)
    
    # write out
    df_con.write.parquet(output_data+"countries.parquet", mode="overwrite")
    if local:
        df_con.write.csv(output_data+"countries.csv", mode="overwrite", header=True, sep=";")
        
  
        
def process_temperature_data(spark, input_data, output_data, local):
    """ Reads temperature fact table
        Writes data to output location in parquet file format

    :params spark: spark session
    :params input_data: filepath csv
    :params output: filepath
    """
    
    temperature_schema = StructType([
        StructField("country_id", StringType(), False),
        StructField("month", StringType(), False),
        StructField("temperature_mean", DoubleType(), False),
        StructField("temperature_min", DoubleType(), False),
        StructField("temperature_max", DoubleType(), False),
    ])
    
    df_temp = spark.read.csv(input_data, sep=";", header=True, schema=temperature_schema)
    
    # write out
    
    df_temp.write.parquet(output_data+"temperature.parquet", mode="overwrite")
    if local:
        df_temp.write.csv(output_data+"temperature.csv", mode="overwrite", header=True, sep=";")


def process_i94_mode_data(spark, output_data, local):
    """ Writes i94modes dimesnion table to output location in parquet file format

    :params spark: spark session
    :params output: filepath
    """

    # i94 modes
    i94mode = [(1, 'Air'),(2,'Sea'),(3,'Land'),(9,'Not reported')]

    i94mode_schema = StructType([       
        StructField('mode_id', IntegerType(), False),
        StructField('mode_desc', StringType(), False),
    ])

    df_mode = spark.createDataFrame(data=i94mode, schema=i94mode_schema)

    # write out
    df_mode = df_mode.repartition(1)
    df_mode.write.parquet(output_data+"mode.parquet", mode="overwrite")
    if local:
        df_mode.write.csv(output_data+"mode.csv", mode="overwrite", header=True, sep=";")


def process_i94_purpose_data(spark, output_data, local):
    """ Writes travel purpose dimesnion table to output location in parquet file format

    :params spark: spark session
    :params output: filepath
    """

    # visa
    i94purpose = [(1, 'Business'),(2,'Pleasure'),(3,'Student'),(9,'Not reported')]

    i94purpos_schema = StructType([       
        StructField('purpose_id', IntegerType(), False),
        StructField('purpose_desc', StringType(), False),
    ])

    df_purpose = spark.createDataFrame(data=i94purpose, schema=i94purpos_schema)

    # write out
    df_purpose = df_purpose.repartition(1)
    df_purpose.write.parquet(output_data+"purpose.parquet", mode="overwrite")
    if local:
        df_purpose.write.csv(output_data+"purpose.csv", mode="overwrite", header=True, sep=";")


def process_i94_data(spark, input_data, output_data, local):
    """ - Reads i94 fact data from filepath
        - Converts datetime formats to date
        - Reads and joins mappings for countries, visa_categories and us_states
        - Cleans i94addr and age columns
        - Creates duration measure
        - Creates date dimension table
        - Writes data to output location in parquet file format:
            - i94, visa_categories, us_states, dates


    :params spark: spark session
    :params input_data: list of filepaths, file order must match execution steps
    :params output_data: filepath
    """

   
    ### read i94 data
    df_spark = get_i94_data(spark, input_data[0])
    
    if local: 
        df_spark = df_spark.limit(300)
    
    
    ### Datetime conversions
    # register udfs
    udf_date_from_sas = udf(lambda x: convert_sas_date(x), DateType())
    udf_date_from_str = udf(lambda x: convert_str_to_date(x), DateType())
    
    # add date columns
    df_spark = df_spark\
        .withColumn("arrival_date", udf_date_from_sas("arrdate")) \
        .withColumn("departure_date", udf_date_from_sas("depdate")) \
        .withColumn("dtadfile_date", udf_date_from_str("dtadfile"))
        
    
    ### i94cit/res number to iso-code mapping
    # read country data
    df_con = get_country_mapping(spark, input_data[1])
    
    # join i94cit
    joinExpr = [df_spark.i94cit == df_con.i94_code]
    df_spark =\
        df_spark.join(df_con.select("i94_code","iso_code"), joinExpr, "left_outer")\
            .withColumn("cit_country_id", coalesce("iso_code", lit(99))).drop("i94_code","iso_code")
    
    # join i94res
    joinExpr = [df_spark.i94res == df_con.i94_code]
    df_spark =\
        df_spark.join(df_con.select("i94_code","iso_code"), joinExpr, "left_outer")\
            .withColumn("res_country_id", coalesce("iso_code", lit(99))).drop("i94_code","iso_code")
  
    
    
    ### Visatype to visa_id mapping
    # read visa
    df_visa = get_visa_mapping(spark, input_data[2])
    joinExpr = [df_spark.visatype == df_visa.visa]
    df_spark =\
        df_spark.join(df_visa.select("visa","visa_id").dropna(), joinExpr, "left_outer")\
            .withColumn("visa_id", coalesce("visa_id", lit(1))).drop("visa")
    
    
    
    ### Clean i94addr - US-States
    df_states = get_us_states_mapping(spark, input_data[3])
    joinExpr = [df_spark.i94addr == df_states.state_id]
    df_spark = \
        df_spark.join(df_states.select("state_id").dropna(), joinExpr, "left_outer")\
            .withColumn("state_id_clean", coalesce("state_id", lit(99))).drop("state_id")
    
    
    ### Clean i94mode - replace nulls with 9 not reported
    df_spark = df_spark.fillna({"i94mode":9})


    ### Clean i94visa - travel purpose - replace nulls with 9 not reported
    df_spark = df_spark.fillna({"i94visa":9})


    ### Clean gender
    udf_clean_gender = udf(lambda x: clean_gender(x), StringType())
    df_spark = df_spark.withColumn("gender_clean", udf_clean_gender("gender"))

    
    ### Clean Age and register udf
    clean_age = udf(lambda x: clean_negative_age(x), IntegerType())
    df_spark = df_spark.withColumn("age", clean_age("i94bir"))
    
    
    ### Create time dimension
    df_dates = df_spark.select(col("arrival_date").alias("date")).dropDuplicates().dropna() \
        .withColumn("year", year("date")) \
        .withColumn("month", month("date")) \
        .withColumn("day", dayofmonth("date")) \
        .withColumn("week", weekofyear("date"))
    
    # Calculate duration in days departure - arrival
    df_spark = df_spark.withColumn("duration", datediff("departure_date","arrival_date"))


    ### Select final fields for fact table
    df_spark = df_spark \
                .withColumn("i94_id", monotonically_increasing_id()) \
                .select(
                        "i94_id",
                        "cit_country_id",
                        "res_country_id",
                        col("state_id_clean").alias("state_id"),
                        col("i94mode").alias("mode_id"),
                        col("i94visa").alias("purpose_id"),
                        "visa_id",
                        "arrival_date",
                        col("cicid").alias("cic_id"),
                        col("gender_clean").alias("gender"),
                        "count",
                        "duration",
                        "age",
                        col("i94yr").alias("year"),
                        col("i94mon").alias("month")
                    )


    ### Write out
    df_spark.write.parquet(output_data+"i94.parquet", mode="append", partitionBy=['year', 'month'] )
    df_visa.write.parquet(output_data+"visa_categories.parquet", mode="overwrite")
    df_states.write.parquet(output_data+"us_states.parquet", mode="overwrite")
    df_dates.write.parquet(output_data+"dates.parquet", mode="append", partitionBy=['year', 'month'] )
    if local:
        df_spark.write.csv(output_data+"i94.csv", header=True, mode="overwrite", sep=";" )
        
        df_visa = df_visa.repartition(1)
        df_visa.write.csv(output_data+"visa_categories.csv", header=True, mode="overwrite", sep=";")
        
        df_states = df_states.repartition(1)
        df_states.write.parquet(output_data+"us_states.parquet", mode="overwrite")
        df_states.write.csv(output_data+"us_states.csv", header=True, mode="overwrite", sep=";" )
        
        df_dates = df_dates.repartition(1)
        df_dates.write.csv(output_data+"dates.csv", header=True, mode="overwrite", sep=";" )
    

def main():
    """ script performs data load from staging into the final datamodel.
        - create spark session
        - read files from given S3 location
        - processe i94 immigration data, country data, temperature data, i94 modes
        - write transformed and cleaned data to S3 location in parquet format

    :params sys.argv[1]: S3 bucket name
    :params Sys.argv[2]: i94 filename following format 'i94_YYYY-MM_sub.sas7bdat' (e. g.'i94_2016-04_sub.sas7bdat')
    """

    # print start-time
    print(datetime.now())    
    
    # flag for local test modus
    local = False
    if local:
        
        # inputs / outputs
        input_data = ["../staging/i94/i94_apr16_sub.sas7bdat", 
                      "../staging/countries_mapping.csv",
                      "../staging/visa_categories.csv",
                      "../staging/us_states.csv",
                      "../staging/countries.csv",
                      "../staging/temperature.csv"]

        output_data = "../model/"
    
    else:
        # inputs via sys args
        bucket_name = sys.argv[1]
        sas_file = sys.argv[2]

        # inputs / outputs
        input_data = []

        input_data.append("s3a://{}/staging/i94/{}".format(bucket_name, sas_file))
        input_data.append("s3a://{}/staging/{}".format(bucket_name, "countries_mapping.csv"))
        input_data.append("s3a://{}/staging/{}".format(bucket_name, "visa_categories.csv"))
        input_data.append("s3a://{}/staging/{}".format(bucket_name, "us_states.csv"))
        input_data.append("s3a://{}/staging/{}".format(bucket_name, "countries.csv"))
        input_data.append("s3a://{}/staging/{}".format(bucket_name, "temperature.csv"))       
    
        output_data = "s3a://{}/model/".format(bucket_name)

        
    # create spark session
    spark = create_spark_session(local)
    
    # process i94
    # writes also visa_categories and time-dimension
    process_i94_data(spark, input_data[0:4], output_data, local)
       
    # process country-continent dimension
    process_country_data(spark, input_data[4], output_data, local)
    
    # process temperature
    process_temperature_data(spark, input_data[5], output_data, local)

    # process i94mode
    process_i94_mode_data(spark, output_data, local)

    # process travel purpose
    process_i94_purpose_data(spark, output_data, local)

    
    # print end-time
    print(datetime.now())


   
if __name__ == "__main__":
    main()