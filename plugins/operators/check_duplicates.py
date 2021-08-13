from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class CheckDuplicatesOperator(BaseOperator):
    """
    Data quality check. It checks that there are no duplicates in table and columns specified by parameters
    'params' must be in a json format, like:
    params = {
    'table1': ['columnX', 'columnY', ],
    'table2': ['columnY', ],
    'table3': ['columnW','columnZ', 'columnK', ],
    }
    
    For each 'table' it check that the corresponding list of columns doesn't have duplicates. 
    The comparison is done on the grouped columns.
    
    """

    ui_color = '#89DA59'
    
    sql_query_base = """SELECT {}, COUNT(*)
                        FROM {}
                        GROUP BY {}
                        HAVING COUNT(*)>1
                        """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = "",
                 params = {},
                 *args, **kwargs):

        super(CheckDuplicatesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.params = params
        

    def execute(self, context):

        redshift_hook = PostgresHook(self.redshift_conn_id)        

              

        for table in self.params.keys():
            
            self.log.info(f"Performing duplicate check on {table}")  
            

            columns_string = CheckDuplicatesOperator.func_generate_string(self.params[table])
            sql_query = CheckDuplicatesOperator.sql_query_base.format(columns_string, table, columns_string)

            self.log.info(f"Running the query:\n {sql_query}")
            records_df = redshift_hook.get_pandas_df(sql_query)
            
            num_records = records_df.shape[0]

            if num_records>0:
                self.log.warning(f"{table} has duplicates in column(s): {self.params[table]}.")
                self.log.info(f"These duplicates have been found:")
                self.log.info(records_df)
            else:
                self.log.info(f"No duplicates found in {table}, column(s): {self.params[table]}.") 

    def func_generate_string(columns_list):
        """
        transform the list of columns in a string of columns separated by commas
        Example: [<column1>, <column2>] -> '<column1>, <column2>'
        """
        mystring2 = ''
        for column in columns_list:
            mystring2 += column + ', '
        return mystring2[:-2]
