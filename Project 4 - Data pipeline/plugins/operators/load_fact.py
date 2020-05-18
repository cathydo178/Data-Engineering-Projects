from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 truncate_table=True,
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate_table = truncate_table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f'Clearing data from truncated table {self.table}')
            redshift.run('DELETE from {}'.format(self.table))
            
            self.log.info(f'data {self.sql}')
            formatted_sql = LoadFactOperator.insert_sql.format(
                self.table,
                self.sql
            )
        redshift.run(formatted_sql)
            
