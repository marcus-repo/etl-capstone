import time
import timeit
import boto3


from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GlueCrawlerOperator(BaseOperator):
    ui_color = '#668EF9'

    @apply_defaults
    def __init__(self,
                 crawler_name="",
                 region_name="",
                 endpoint_url="",
                 *args, **kwargs):

        super(GlueCrawlerOperator, self).__init__(*args, **kwargs)
        self.crawler_name = crawler_name
        self.region_name = region_name
        self.endpoint_url = endpoint_url



    def glue_crawl(self, client, crawler, timeout_minutes = 5, retry_seconds= 5):
        """Crawl the specified AWS Glue crawler
           Wait until crawler is ready.
           Start crawling and monitor crawling process until done. 

        :params client: S3 glue cient
        :params crawler: S3 aws glue crawler name (must be created upfront)
        :params timeout_minutes: waiting time until process is canceled
        :params retry_seconds: waiting time for next status checks of the crawler
        """
        
        # define timeout
        timeout_seconds = timeout_minutes * 60
        start_time = timeit.default_timer()
        abort_time = start_time + timeout_seconds
        


        # test for timeout
        def check_for_timeout():
            if timeit.default_timer() > abort_time:
                raise TimeoutError(f"Failed to crawl {crawler}. The allocated time of {timeout_minutes:,} minutes has elapsed.")
        
        # expected behaviour : ready, running, stopping, ready
        def wait_until_ready():
            state_previous = None
            while True:
                response_get = client.get_crawler(Name=crawler)
                state = response_get["Crawler"]["State"]
                if state != state_previous:
                    print(f"Crawler {crawler} is {state.lower()}.")
                    state_previous = state
                if state == "READY":  # Other known states: RUNNING, STOPPING
                    return
                check_for_timeout()
                time.sleep(retry_seconds)
        
        
        # init crawler - status ready
        wait_until_ready()
        
        # start crawling if ready
        response_start = client.start_crawler(Name=crawler)
        
        # check if successfully started
        if response_start["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(f"Crawler {crawler} is started.")
        
        # wait unitl ready againg
        wait_until_ready()

        # Crawling completed.
        print(f"Crawler {crawler} is finished.")


    def execute(self, context):
        """ Initiate glue crawler and runs crawler.
        """

        # init client
        client = boto3.client(service_name='glue', 
                              region_name=self.region_name,
                              endpoint_url=self.endpoint_url)
        
        # run crawler
        self.glue_crawl(client, self.crawler_name)