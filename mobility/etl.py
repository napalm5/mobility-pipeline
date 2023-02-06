from os.path import join as pjoin
import logging

from airflow.contrib.hooks.vertica_hook import VerticaHook


def vertica_etl(queries_dir='./queries',**kwargs):
    print('ciao')
    cur = VerticaHook('VerticaProd').get_cursor()
    
    with open(pjoin(queries_dir,'set_vertica_parameters.sql'), 'r') as file:
        query_setup = file.read()
    with open(pjoin(queries_dir,'get_mobility_data_aggregated.sql'), 'r') as file:
        query = file.read()

    cur.execute(query_setup)
    result_setup = cur.fetchall()
    cur.execute(query)
    result = cur.fetchall()
    logging.info(result_setup)
    logging.info(result)
    logging.info('Nb of lines saved')
