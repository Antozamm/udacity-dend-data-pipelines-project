import os
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


from helpers import SqlQueries


class StageToRedshiftOperator(BaseOperator):
    """
    This operator is used to stage S3 data to AWS Redshift
    Arguments:
    - aws_credentials
    - redshift_conn_id
    - kwargs, contains the parameters to customize the SQL query 'copy_sql_query', in a json alike format
    """
    
    ui_color = '#358140'
    
    copy_sql_query = SqlQueries.copy_stage_table
    

    @apply_defaults
    def __init__(self,
                 aws_credentials = '',
                 redshift_conn_id = '',
                 params = {},
                 *args,
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials = aws_credentials
        self.redshift_conn_id = redshift_conn_id
        self.kwargs = kwargs
    

        

    def execute(self, context):
        
        self.log.info('Hooking AWS credentials')
        aws_hook = AwsHook(self.aws_credentials)
        credentials =  aws_hook.get_credentials()
        
        if self.kwargs['credential_access_key']=='':
            self.kwargs['credential_access_key'] = credentials.access_key
            
        if self.kwargs['credential_secret_key']=='':
            self.kwargs['credential_secret_key'] = credentials.secret_key


        self.log.info('Connecting to AWS Redshift')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info('StageToRedshift Operator - Executing SQL COPY into') 
        sql_query = StageToRedshiftOperator.copy_sql_query.format(self.kwargs['table'],
                                                                    self.kwargs['s3_key'],
                                                                    self.kwargs['credential_access_key'],
                                                                    self.kwargs['credential_secret_key'],
                                                                    self.kwargs['region'],
                                                                    self.kwargs['json_s3_key'])
        self.log.info(sql_query)
        redshift.run(sql_query)


        


