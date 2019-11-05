import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import os


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
import pandas as pd

import unittest



from accuracy import *

class AccuracyTest(unittest.TestCase):

    def test_accuracy(self):
        data = [
            {"row_id": 1, "name": "Robin"},
            {"row_id": 2, "name": "Robin"},
            {"row_id": 3, "name": "Robin"},
            {"row_id": 4, "name": "Robin"}
        ]
        df = pd.DataFrame(data)
        df = spark.createDataFrame(df)

        data = [
            {"row_id": 1, "component": 0, "group": 0},
            {"row_id": 2, "component": 0, "group": 0},
            {"row_id": 3, "component": 1, "group": 0},
            {"row_id": 4, "component": 0, "group": 1}
        ]
        cc = pd.DataFrame(data)
        cc = spark.createDataFrame(cc)

        pairs = get_real_pred_pairs(cc, spark)
        non_cartesian = get_accuracy_statistics(pairs, df, spark)

        pairs = get_cartesian_pairs(cc, spark)
        pairs = categorise_pairs(pairs, spark)

        cartesian = get_accuracy_statistics_cartesian(pairs, spark)
        assert non_cartesian==cartesian
