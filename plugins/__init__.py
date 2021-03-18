from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
#import helpers

# Defining the plugin class
class MyPlugin(AirflowPlugin):
    name = "my_plugin"
    operators = [
        operators.StageToS3Operator,
        operators.GlueCrawlerOperator
    ]
