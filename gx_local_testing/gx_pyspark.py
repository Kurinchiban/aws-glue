from pyspark.sql import SparkSession
import great_expectations as gx
import json

# Step 1: Initialize Spark session
spark = SparkSession.builder \
    .appName("GreatExpectationsSpark") \
    .getOrCreate()

# Step 2: Read the CSV using PySpark
csv_path = "/home/kurinchiban/Desktop/glue-job/employee.csv"
spark_df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)

# Step 3: Initialize Great Expectations context
context = gx.get_context()

# Step 4: Define a Spark Data Source
data_source_name = "spark_datasource"
data_asset_name = "employee_asset"
batch_definition_name = "spark_batch"

data_source = context.data_sources.add_spark(name=data_source_name)
data_asset = data_source.add_dataframe_asset(name=data_asset_name)
batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

# Step 5: Define expectation on the "emp_id" column
expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column="emp_id")

# Step 6: Create batch and validate
batch_parameters = {"dataframe": spark_df}
batch = batch_definition.get_batch(batch_parameters=batch_parameters)
validation_results = batch.validate(expectation)

validation_json = json.dumps(validation_results.to_json_dict(), indent=2)

# Write validation results to S3 using Spark
spark.sparkContext.parallelize([validation_json]).coalesce(1).saveAsTextFile("/home/kurinchiban/Desktop/glue-job/gx_local_testing/validation_result.txt")

# Step 7: Show results
# print(validation_results)