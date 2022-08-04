from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CheckDuplicatesOperator)

from airflow.contrib.hooks.aws_hook import AwsHook


from helpers import SqlQueries



# Create a connection:
# airflow connections -a --conn_id 'aws_credentials' --conn_type 'aws' --conn_login 'xyz' --conn_password 'xyz123'
# airflow connections -a --conn_id 'redshift' --conn_type 'postgres' --conn_host 'redshift-cluster-1.cugnyqpetzqp.us-west-2.redshift.amazonaws.com' --conn_schema 'dev' --conn_login 'awsuser' --conn_password 'Giulia83' --conn_port 5439
# /opt/airflow/start.sh

# Setting DAG defaults
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes = 1),
    
}

dag = DAG('sparkify_etl',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *',
#          schedule_interval=None,
          catchup= False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Setting kwargs for staging_events
kwargs = {'table': 'staging_events',
          's3_key': 's3://udacity-dend/log_data',
          'credential_access_key': '',
          'credential_secret_key': '',
          'region': 'us-west-2',
          'json_s3_key': 's3://udacity-dend/log_json_path.json'}
    
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials = 'aws_credentials',
    redshift_conn_id = 'redshift',
    **kwargs
)

# Setting kwargs for staging_songs
kwargs = {'table': 'staging_songs',
          's3_key': 's3://udacity-dend/song_data/',
          'credential_access_key': '',
          'credential_secret_key': '',
          'region': 'us-west-2',
          'json_s3_key': 'auto'}

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Staging_songs',
    dag=dag,
    aws_credentials = 'aws_credentials',
    redshift_conn_id = 'redshift',
    **kwargs
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    sql_query = SqlQueries.songplay_table_insert
    
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    sql_query = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    sql_query = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    sql_query = SqlQueries.artist_table_insert

)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,    
    sql_query = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    tables = ['songplays', 'users', 'songs', 'time', 'artists' ],
    op_kwargs = default_args,
    retries = 3,
    retry_delay= timedelta(minutes = 1)
)

run_check_duplicates = CheckDuplicatesOperator(
    task_id='Run_check_duplicates',
    dag=dag,
    redshift_conn_id = 'redshift',
    params = {'artists' :['name',],
              'songs'   :['artistid','title']}
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# setting task priorities

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift  >> load_songplays_table

load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table

load_time_dimension_table   >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_song_dimension_table   >> run_quality_checks
load_user_dimension_table   >> run_quality_checks

run_quality_checks    >> run_check_duplicates       

run_check_duplicates  >> end_operator


