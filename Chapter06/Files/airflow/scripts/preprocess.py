import sys
import os
import boto3
import pyspark
import pandas as pd
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import Pipeline
from pyspark.sql.types import StructField, StructType, StringType, DoubleType
from pyspark.ml.feature import StringIndexer, VectorIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql.functions import *
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions

def csv_line(data):
    r = ','.join(str(d) for d in data[1])
    return str(data[0]) + "," + r
    
def toS3(df, path):
    rdd = df.rdd.map(lambda x: (x.rings, x.features))
    rdd_lines = rdd.map(csv_line)
    spark_df = rdd_lines.map(lambda x: str(x)).map(lambda s: s.split(",")).toDF()
    pd_df = spark_df.toPandas()
    pd_df = pd_df.drop(columns=["_3"])
    pd_df.to_csv(f"s3://{path}", header=False, index=False)

def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = SparkSession.builder.appName("PySparkAbalone").getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
    args = getResolvedOptions(sys.argv, ["GLUE_CATALOG", "S3_BUCKET", "S3_INPUT_KEY_PREFIX", "S3_OUTPUT_KEY_PREFIX"])
    schema = StructType(
        [
            StructField("sex", StringType(), True),
            StructField("length", DoubleType(), True),
            StructField("diameter", DoubleType(), True),
            StructField("height", DoubleType(), True),
            StructField("whole_weight", DoubleType(), True),
            StructField("shucked_weight", DoubleType(), True),
            StructField("viscera_weight", DoubleType(), True),
            StructField("shell_weight", DoubleType(), True),
            StructField("rings", DoubleType(), True)
        ]
    )
    columns = ["sex", "length", "diameter", "height", "whole_weight", "shucked_weight", "viscera_weight", "shell_weight", "rings"]
    new = glueContext.create_dynamic_frame_from_catalog(database=args["GLUE_CATALOG"], table_name="new", transformation_ctx="new")
    new_df = new.toDF()
    new_df = new_df.toDF(*columns)
    raw_df = spark.read.csv(("s3://{}".format(os.path.join(args["S3_BUCKET"], args["S3_INPUT_KEY_PREFIX"]))), header=False, schema=schema)
    merged_df = reduce(DataFrame.unionAll, [raw_df, new_df])
    distinct_df = merged_df.distinct()
    sex_indexer = StringIndexer(inputCol="sex", outputCol="indexed_sex")
    sex_encoder = OneHotEncoder(inputCol="indexed_sex", outputCol="sex_vec")
    assembler = VectorAssembler(
        inputCols=[
            "sex_vec",
            "length",
            "diameter",
            "height",
            "whole_weight",
            "shucked_weight",
            "viscera_weight",
            "shell_weight"
        ],
        outputCol="features"
    )
    pipeline = Pipeline(stages=[sex_indexer, sex_encoder, assembler])
    model = pipeline.fit(distinct_df)
    transformed_df = model.transform(merged_df)
    (train_df, validation_df, test_df) = transformed_df.randomSplit([0.8, 0.15, 0.05])
    toS3(train_df, os.path.join(args["S3_BUCKET"], args["S3_OUTPUT_KEY_PREFIX"], "training/training.csv"))
    toS3(validation_df, os.path.join(args["S3_BUCKET"], args["S3_OUTPUT_KEY_PREFIX"], "training/validation.csv"))
    toS3(test_df, os.path.join(args["S3_BUCKET"], args["S3_OUTPUT_KEY_PREFIX"], "testing/testing.csv"))

if __name__ == "__main__":
    main()