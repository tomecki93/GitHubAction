import sys
import snowflake.connector as sf
# from awsglue.utils import getResolvedOptions


class tech_func:
    def __init__(self):
        pass

    @staticmethod
    def sf_connection(connection, query):
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            cursor.close()
        except Exception as e:
            print(e)


def main_load_dtm_stores():
    # args = getResolvedOptions(sys.argv, ['ACCOUNT', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD'])

    conn = sf.connect(
        user='TOMECKI1993',
        password='Abis1993',
        account='sqa68179',
        warehouse='DWH_STORES',
        database='STORE_DB',
        schema='DATA_MART'
    )

    # Logged stage
    job_id = '1.0.0.0.STG'
    jobs_name = ['stage_tmp_stores', 'target_dim_stores']

    sql = "call STORE_DB.META.META_START_PROCESS('{}','{}')".format(job_id, jobs_name[0])
    tech_func.sf_connection(conn, sql)

    sql = "call STORE_DB.META.META_END_PROCESS('{}','{}')".format(job_id, jobs_name[0])
    tech_func.sf_connection(conn, sql)

    # Start load to target
    job_id = '1.0.0.0.DTM'

    sql = "call STORE_DB.META.META_START_PROCESS('{}','{}')".format(job_id, jobs_name[1])
    tech_func.sf_connection(conn, sql)

    sql = "call STORE_DB.DATA_MART.DIM_STORES_LOAD('{}')".format(job_id)
    tech_func.sf_connection(conn, sql)

    sql = "call STORE_DB.META.META_END_PROCESS('{}','{}')".format(job_id, jobs_name[1])
    tech_func.sf_connection(conn, sql)

main_load_dtm_stores()