from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    load_fact_sql = """
        INSERT INTO {}
        {}
    """

    delete_fact_sql = """
        DELETE FROM {}
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 operation="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.operation = operation

    def execute(self, context):
        # create redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # evaluate operation
        if self.operation == "load":
            # load fact table
            formatted_sql = LoadFactOperator.load_fact_sql.format(
                self.table,
                self.sql_query
            )
            redshift.run(formatted_sql)
        elif self.operation == "delete":
            # delete fact table
            formatted_sql = LoadFactOperator.delete_fact_sql.format(
                self.table
            )
            redshift.run(formatted_sql)
        else:
            raise ValueError(f"Invalid operation: {self.operation}")

