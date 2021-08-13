from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Data quality check on the tables created.
    It checks that all tables habe been populated, number of record higher than 0
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        
        redshift_hook = PostgresHook(self.redshift_conn_id)        

        for table in self.tables:
            self.log.info(f"Performing data quality check on {table}")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no result.")
                                
            num_records = records[0][0]
                
            if num_records > 0:
                self.log.info(f"Found {num_records} rows in {table}. Data quality check passed.")
            else:
                raise ValueError(f"Data quality check failed. {table} returned 0 rows.")