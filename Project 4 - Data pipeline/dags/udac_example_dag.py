from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'cathup': False,
    'email_on_retry': False,
    'start_date': datetime(2019, 1, 12)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task=PostgresOperator(
    task_id="create_tables_task",
    dag=dag,
    sql="create_tables.sql",
    postgres_conn_id="redshift"
)

stage_events_to_redshift =StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table='public.staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json="s3://udacity-dend/log_json_path.json",
    region="us-west-2"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.staging_songs',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data/A/A/A',
    json="auto",
    region="us-west-2"
)
   
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="public.songplays",
    truncate_table=True,
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="public.users",
    truncate_table=True,
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="public.songs",
    truncate_table=True,
    sql=SqlQueries.song_table_insert
    
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="public.artists",
    truncate_table=True,
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="public.time",
    truncate_table=True,
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift >> load_songplays_table
create_tables_task >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
