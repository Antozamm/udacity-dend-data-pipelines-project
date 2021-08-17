from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This operator executes the sql_query passed as argument.
    It is used to load the dimension tables 
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 sql_query = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query 

    def execute(self, context):
        
        self.log.info('LoadDimensionOperator - Executing query {self.sql_query}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(self.sql_query)
