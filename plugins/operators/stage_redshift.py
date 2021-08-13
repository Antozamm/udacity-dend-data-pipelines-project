import os
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


    


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials = '',
                 redshift_conn_id = '',
                 sql_query = '',
                 *args,
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials = aws_credentials
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        

    def execute(self, context):
        
        aws_hook = AwsHook('aws_credentials')
        credentials =  aws_hook.get_credentials()
     
        self.log.info('Connecting to AWS Redshift')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info('StageToRedshift Operator - Executing SQL COPY into') 

        self.log.info(type(self.sql_query))
        self.log.info(self.sql_query)
        redshift.run(self.sql_query)


        


