from os.path import join as pjoin

def vertica_etl(queries_dir='./queries',**kwargs):
    cur = VerticaHook('VerticaProd').get_cursor()
    
    with open(pjoin(queries_dir,'set_vertica_parameters.sql'), 'r') as file:
        query_setup = file.read()
    with open(pjoin(queries_dir,'get_mobility_data.sql'), 'r') as file:
        query = file.read()

    cur.execute(query_setup)
    result_setup = cur.fetchall()
    cur.execute(query)
    result = cur.fetchall()
    logging.info(result_setup)
    logging.info(result)
    logging.info('Nb of lines saved')
