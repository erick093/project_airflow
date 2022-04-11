from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    load_dimension_sql = """
        INSERT INTO {}
        {}
    """

    truncate_dimension_sql = """
        TRUNCATE TABLE {}
    """


    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 operation="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.operation = operation
        self.sql_query = sql_query


    def execute(self, context):
        # create redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # validate operation
        if self.operation == "load":
            self.log.info(f"Loading data into {self.table}")
            # insert data into dimension table
            formatted_sql = LoadDimensionOperator.load_dimension_sql.format(
                self.table,
                self.sql_query
            )
            redshift.run(formatted_sql)
            self.log.info(f"Inserted data into {self.table}")
        elif self.operation == "truncate":
            # truncate dimension table
            self.log.info(f"Truncating {self.table}")
            formatted_sql = LoadDimensionOperator.truncate_dimension_sql.format(
                self.table
            )
            redshift.run(formatted_sql)
            self.log.info(f"Truncated {self.table}")
        else:
            raise ValueError(f"Invalid operation: {self.operation}")

