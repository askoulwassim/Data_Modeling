from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

"""
    Operator to load dimension tables from fact table.
"""

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",
                 append_mode = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_mode = append_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_mode == False:
            self.log.info(f"Deleting {self.table} table...")
            redshift.run(f"DELETE FROM {self.table}")
            
        load_query = getattr(SqlQueries,self.sql).format(self.table)
        redshift.run(load_query)
        
        self.log.info(f'Loaded {self.table} to dim table')