from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    # has rows quality check
    has_rows_sql = """
        SELECT COUNT(*)
        FROM {table}
    """

    # has nulls quality check
    has_nulls_sql = """
        SELECT COUNT(*)
        FROM {table}
        WHERE {column} IS NULL
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables_columns={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_columns = tables_columns

    def execute(self, context):

        # create redshift hook
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # iterate over items in dictionary
        for table, columns in self.tables_columns.items():
            # check if table has rows
            records = redshift_hook.get_records(self.has_rows_sql.format(table=table))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            # check if table has nulls in columns
            for column in columns:
                null_records = redshift_hook.get_records(self.has_nulls_sql.format(table=table, column=column))
                num_nulls = null_records[0][0]
                if num_nulls > 0:
                    raise ValueError(f"Data quality check failed. {table} contained nulls in column {column}")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records and 0 nulls")
        self.log.info("Data quality check passed")