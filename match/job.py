import sys
import os
import json

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, CountVectorizer
from pyspark.ml.classification import LogisticRegression

from utility_functions import *
from sql_steps import *
from pipelines import get_features_df



sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
sc.setCheckpointDir('/tmp/')
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'test_arg'])

logger.info("JOB SPECS...")
logger.info("JOB_NAME: " + args["JOB_NAME"])
logger.info("test argument: " +  args["test_arg"])

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


logger.info("Starting to read data")
df = spark.read.parquet(
    "s3a://alpha-data-linking/nonsensitive_test_data/1million/")
num_records = df.count()
logger.info(str(num_records))

df = df.repartition(25)

logger.info("Read data")

train, test = df.randomSplit([0.5, 0.5], seed=12345)

train_features = get_features_df(train, spark)
test_features = get_features_df(test, spark)


lr = LogisticRegression(maxIter=10, regParam=0.01,
                        featuresCol="features", labelCol="real_group")
lr_model = lr.fit(train_features)
# # Print the coefficients and intercept for logistic regression
logger.info("Coefficients: " + str(lr_model.coefficients))
logger.info("Intercept: " + str(lr_model.intercept))

predictions = lr_model.transform(test_features)

predictions.write.parquet(
    "s3a://alpha-data-linking/nonsensitive_test_data/delete/predictions", mode="overwrite")


metrics = get_metrics(predictions, spark)

logger.info(json.dumps(metrics, indent=4))

