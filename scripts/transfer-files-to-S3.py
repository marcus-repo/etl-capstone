import configparser
import os
import glob


import logging
import boto3
from botocore.exceptions import ClientError

import re
from datetime import datetime


# from os import listdir
# from os.path import isfile, join

# read configurations
config = configparser.ConfigParser()
config.read_file(open('aws.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
BUCKET_NAME = config.get('S3', 'BUCKET_NAME')
LOCAL_DATA = config.get('LOCAL', 'LOCAL_DATA')



def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3',     
                                aws_access_key_id=KEY,
                                aws_secret_access_key=SECRET)
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        

def list_files(filepath, ext):
    """Returns list of filenames in a given directory
    
    :param filepath: path to files
    :param ext: file-extension to search for e.g. "*.csv"
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, ext))
        for f in files:
            # get filename
            base=os.path.basename(f)
            # get absolute path
            abs_path = os.path.abspath(f)
            all_files.append((abs_path, base))
    
    return all_files


def transfer_csv_files(filepath):
    """ Transfers csv files in a given path to staging S3 bucket
    
    :param filepath: path to files
    """
    
    # pass paramaters to list_files function
    files = list_files(filepath, '*.csv')    
    for path, filename in files:
        print(filename)
        upload_file(path, BUCKET_NAME , 'staging/' + filename)
    

def transfer_sas_files(filepath, year, month):
    """ - Search filepath for a sas7bdat file for a given year/month
        - Replace short month-name/year (2 ditgits) with year-month 
          (e. g. apr16 -> 2016-04) 
        - Transfer file to to staging S3 bucket
        
    :param filepath: path to files
        filenames must follow pattern:
        (i94_)([a-z]{3})([0-9]{2})(_sub\.sas7bdat)
        e. g. i94_apr2016_sub.sas7bdat
    :param year: year to be searched for in filename
    :param month: month to b searched for in filename
    """
    
    # pass paramaters to list_files
    files = list_files(filepath, '*.sas7bdat')    
    
    # loop through sas files
    for i in range(len(files)):
        
        # extract path and file
        path = files[i][0]
        filename = files[i][1]
        
        # extract groups from file pattern to replace year and month
        pattern = re.compile("(i94_)([a-z]{3})([0-9]{2})(_sub\.sas7bdat)")
        result = pattern.search(filename)
        if result:

            # group 1 : prefix
            # group 2 : monthname short
            # group 3 : year 2-digits
            # group 4 : suffix
            
            # extract month from group 2
            month_number = datetime.strptime(result.group(2), "%b").month
            month_str = '0' + str(month_number)
            month_str = month_str[-2:]
            
            # extract year from group 3
            year_str = str(datetime.strptime(result.group(3), "%y").year)
            
            
            # build new file name / object_name
            temp_str = "\g<1>" + year_str + '-' + month_str + "\g<4>"
            object_name = pattern.sub(temp_str, filename)
            
            
            # upload matched file 
            if year_str == year and month_str == month:
            # load specific year/month

                print(f"month {object_name}")
                upload_file(path, BUCKET_NAME, 'staging/i94/'+ object_name)
                
                # if found exit loop
                break
            
            #print(object_name)
            

def main():
    """ Script handles the file transfer local staging to S3 staging.
        - Read csv files from a given filepath.
        - Upload csv files to given AWS S3 bucket.
        - Read sas files from a given filepath.
        - Upload sas files with airflow-ready filename to S3 bucket.
    
    """
    
    # local path ".\staging\"
    filepath = LOCAL_DATA
    year = '2016'
    month ='01'
    
    try:
        # read csv files from path and insert into S3 bucket
        transfer_csv_files(filepath)
        
        # read sas files from path and insert into S3 bucket
        transfer_sas_files(filepath, year=year, month=month)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
