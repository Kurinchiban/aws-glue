import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from utils.connector_utils import (
    read_csv_with_header,
    write_csv_with_header
)

from utils.timer_utils import (
    get_current_timestamp
)

from utils.transfromation_utils import (
    deduplicate_by_columns
)

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parameters
timestamp = get_current_timestamp()
input_path = "s3://glue-demo-bucket-150425/input/employees.csv"
output_path = f"s3://glue-demo-bucket-150425/output/unique_employees/{timestamp}/"

# Read, deduplicate, and write
df = read_csv_with_header(spark, input_path)
deduped_df = deduplicate_by_columns(df, ["first_name", "last_name"])
write_csv_with_header(deduped_df, output_path)

job.commit()


# To create a Job

# aws glue create-job \
#   --name my-glue-job-v2 \
#   --role Reveal-Glue-S3 \
#   --command '{"Name":"glueetl","ScriptLocation":"s3://glue-demo-bucket-150425/script/unique_employees_glue_job_v2.py","PythonVersion":"3"}' \
#   --default-arguments '{
#     "--TempDir":"s3://aws-glue-assets-954147952259-us-east-2/",
#     "--job-language":"python",
#     "--extra-py-files":"s3://glue-demo-bucket-150425/requirements/utils.zip"
#   }' \
#   --glue-version "5.0" \
#   --region us-east-2




# To Run a job

# aws glue start-job-run-v2 \
#   --job-name my-glue-job \
#   --arguments '{"--TempDir":"s3://aws-glue-assets-954147952259-us-east-2/","--job-bookmark-option":"job-bookmark-disable"}' \
#   --region us-east-2