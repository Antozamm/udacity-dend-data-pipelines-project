from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    This operator executes the sql_query passed as argument.
    It is used to load the fact tables songplays
    """

    ui_color = '#F98866'


    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 sql_query = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query         

    def execute(self, context):
     
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"LoadFactOperator - Executing query: {self.sql_query}")
        redshift.run(self.sql_query)