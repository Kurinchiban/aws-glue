import sys
import json
import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import great_expectations as gx

# Initialize
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parameters
timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
input_path = "s3://glue-demo-bucket-150425/input/employees.csv"
output_path = f"s3://glue-demo-bucket-150425/output/unique_employees/{timestamp}/"
validation_path = f"s3://glue-demo-bucket-150425/validation/employee_validation_{timestamp}.txt"

# Read CSV
df = spark.read.option("header", "true").csv(input_path)

# Drop duplicates based on first_name and last_name
deduped_df = df.dropDuplicates(["first_name", "last_name"])

# Initialize Great Expectations context
context = gx.get_context()

# Define a Spark Data Source
data_source_name = "spark_datasource"
data_asset_name = "employee_asset"
batch_definition_name = "spark_batch"

data_source = context.data_sources.add_spark(name=data_source_name)
data_asset = data_source.add_dataframe_asset(name=data_asset_name)
batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

# Define expectation on the "emp_id" column
expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column="emp_id")

# Create batch and validate
batch_parameters = {"dataframe": deduped_df}
batch = batch_definition.get_batch(batch_parameters=batch_parameters)
validation_results = batch.validate(expectation)

# Serialize validation results to JSON
validation_json = json.dumps(validation_results.to_json_dict(), indent=2)

# Write Validation result
spark.sparkContext.parallelize([validation_json]).coalesce(1).saveAsTextFile(validation_path)

# Write to S3
deduped_df.write.mode("overwrite").option("header", "true").csv(output_path)

job.commit()


# aws glue create-job \
#   --name my-glue-job-v1 \
#   --role Reveal-Glue-S3 \
#   --command '{"Name":"glueetl","ScriptLocation":"s3://glue-demo-bucket-150425/script/unique_employees_glue_job_v1.py","PythonVersion":"3"}' \
#   --default-arguments '{
#     "--TempDir":"s3://aws-glue-assets-954147952259-us-east-2/",
#     "--job-language":"python",
#     "--extra-py-files":"s3://glue-demo-bucket-150425/requirements/python_libs.zip"
#   }' \
#   --glue-version "5.0" \
#   --region us-east-2
  
# aws glue start-job-run \
#   --job-name my-glue-job-v1 \
#   --arguments '{"--TempDir":"s3://aws-glue-assets-954147952259-us-east-2/","--job-bookmark-option":"job-bookmark-disable"}' \
#   --region us-east-2