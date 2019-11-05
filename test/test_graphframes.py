import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import os


sc = SparkContext.getOrCreate()

spark = SparkSession(sc)
spark.sparkContext.setCheckpointDir('/tmp/spark_temp/')
import pandas as pd

import unittest



# Need to add the tests dirs to the path so we can see the test helpers dir
sys.path.append(os.path.dirname(__file__))

# Need to add the glue_py_resources file so we can access the job functions
project_root_dir = os.getcwd()
job_dir = os.path.join(project_root_dir, "match", "glue_py_resources")
sys.path.append(job_dir)


from graphframes import GraphFrame
class GFTest(unittest.TestCase):

    def test_gf(self):


        vertices = spark.createDataFrame([('1', 'Carter', 'Derrick', 50),
                                        ('2', 'May', 'Derrick', 26),
                                        ('3', 'Mills', 'Jeff', 80),
                                        ('4', 'Hood', 'Robert', 65),
                                        ('5', 'Banks', 'Mike', 93),
                                        ('98', 'Berg', 'Tim', 28),
                                        ('99', 'Page', 'Allan', 16)],
                                        ['id', 'name', 'firstname', 'age'])
        edges = spark.createDataFrame([('1', '2', 'friend'),
                                    ('2', '1', 'friend'),
                                    ('3', '1', 'friend'),
                                    ('1', '3', 'friend'),
                                    ('2', '3', 'follows'),
                                    ('3', '4', 'friend'),
                                    ('4', '3', 'friend'),
                                    ('5', '3', 'friend'),
                                    ('3', '5', 'friend'),
                                    ('4', '5', 'follows'),
                                    ('98', '99', 'friend'),
                                    ('99', '98', 'friend')],
                                    ['src', 'dst', 'type'])
        g = GraphFrame(vertices, edges)
        g.connectedComponents().show()
