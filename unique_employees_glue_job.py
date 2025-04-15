import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parameters
input_path = "s3://glue-demo-bucket-150425/input/employees.csv"
output_path = "s3://glue-demo-bucket-150425/output/unique_employees/"

# Read CSV
df = spark.read.option("header", "true").csv(input_path)

# Drop duplicates based on first_name and last_name
deduped_df = df.dropDuplicates(["first_name", "last_name"])

# Write to S3
deduped_df.write.mode("overwrite").option("header", "true").csv(output_path)

job.commit()


# To create a Job

# aws glue create-job \
#   --name my-glue-job \
#   --role Reveal-Glue-S3 \
#   --command '{"Name":"glueetl","ScriptLocation":"s3://glue-demo-bucket-150425/script/unique_employees_glue_job.py","PythonVersion":"3"}' \
#   --default-arguments '{"--TempDir":"s3://aws-glue-assets-954147952259-us-east-2/","--job-language":"python"}' \
#   --glue-version "5.0" \
#   --region us-east-2

# To Run a job

# aws glue start-job-run \
#   --job-name my-glue-job \
#   --arguments '{"--TempDir":"s3://aws-glue-assets-954147952259-us-east-2/","--job-bookmark-option":"job-bookmark-disable"}' \
#   --region us-east-2