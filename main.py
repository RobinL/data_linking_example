from etl_manager.etl import GlueJob
import os

try:
    ROLE = os.environ["GLUE_ROLE"]
except:
    raise Exception("You must provide a role name")

bucket = 'alpha-data-linking'

job = GlueJob('match/', bucket=bucket, job_role=ROLE,
              job_arguments={"--test_arg": 'some_string',
                             "--conf": 'spark.jars.packages=graphframes:graphframes:0.6.0-spark2.3-s_2.11',
                             '--enable-spark-ui': 'true',
                             '--spark-event-logs-path': 's3://alpha-data-linking/glue_test_delete/logsdelete',
                             '--enable-continuous-cloudwatch-log': 'true'})

job.job_name = '1m_p_50_e_6'
print(job._job_definition())

job.allocated_capacity = 2

try:
    job.run_job()
    job.wait_for_completion()
finally:
    pass
    # job.cleanup()

