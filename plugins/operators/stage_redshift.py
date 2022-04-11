from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        REGION '{}'
        COMPUPDATE OFF
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 ignore_headers=1,
                 region="us-west-2",
                 *args, **kwargs):

        # get the super class init method
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        # Mapping params
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.ignore_headers = ignore_headers
        self.region = region

    def execute(self, context):

        # creating aws hook and getting credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        # creating redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # truncate data from table
        # self.log.info("Clearing data from destination Redshift table")
        # redshift.run("TRUNCATE TABLE {}".format(self.table))

        # copying data from s3 to redshift
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)  # context is a dictionary
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path,
            self.region
        )

        # executing copy command
        redshift.run(formatted_sql)