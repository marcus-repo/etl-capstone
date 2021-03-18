import os
import glob
import re
from datetime import datetime


from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToS3Operator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 mode="staging",
                 filename=[],   # path or list of files
                 bucket_name="",
                 prefix="",
                 key="",
                 load_sas=False,
                 *args, **kwargs):

        super(StageToS3Operator, self).__init__(*args, **kwargs)
        self.mode = mode
        self.filename = filename
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.key = key
        self.load_sas = load_sas
        
        
        
    def list_files(self, filepath, ext):
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


    def transfer_csv_files(self, filepath):
        """ Transfers csv files in a given path to staging S3 bucket
    
        :param filepath: path to files
        """
        
        # get csv files
        files = self.list_files(filepath, '*.csv')    
        for path, filename in files:
            print(filename)
            self.upload(filename=path, 
                        bucket_name=self.bucket_name, 
                        key= f"{self.prefix}/{filename}", 
                        replace=True)
            
            
    def transfer_sas_files(self, filepath, year=None, month=None):
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
        files = self.list_files(filepath, '*.sas7bdat')    
        
        # loop through sas files
        for i in range(len(files)):
            
            
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
    
                    print(f"sas-file: {object_name}")
                    self.upload(filename=path, 
                                bucket_name=self.bucket_name, 
                                key= f"{self.prefix}/i94/{object_name}", 
                                replace=True)
                    
                    # if found exit loop
                    break
                
                #print(object_name)
    
    
    def upload(self, filename, bucket_name, key, replace):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param key: S3 key
        :param replace: overwrite existing object
        """
        
        s3 = S3Hook()
        s3.load_file(filename=filename, 
                    bucket_name=bucket_name, 
                    key=key, 
                    replace=replace)
        

    def execute(self, context):
        """ executes upload from local staging/scripts to S3 staging/scripts
            if mode="staging" then datafiles csv/sas are uploaded
            if mode="scripts" then scriptfiles are uploaded

        :params context: context['prev_execution_date'] to determine year and month
                         of relevant SAS file
        """
        
        # if mode is staging relevant data is uploaded
        if self.mode=="staging":
            filepath = self.filename[0]
            
            # transfer csv
            self.transfer_csv_files(filepath)
            
            
            # transfer sas if TRUE
            if self.load_sas:

                year = str(context['prev_execution_date'].year)
                month = '{:02}'.format(context['prev_execution_date'].month)
                
                print(year, month)
                self.transfer_sas_files(filepath, year, month)
        
        # transfer scripts
        elif self.mode=="scripts":
            for f in self.filename:
                
                object_name = os.path.split(f)[1]
                self.upload(filename=f, 
                            bucket_name=self.bucket_name, 
                            key= f"{self.prefix}/{object_name}",
                            replace=True)
          
        else:
            raise ValueError(f"Unknown Mode parameter: {self.mode}")
