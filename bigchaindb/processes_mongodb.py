import logging
import multiprocessing as mp

import bigchaindb
from bigchaindb.pipelines import block_mongodb as block
from bigchaindb.web import server


logger = logging.getLogger(__name__)

BANNER = """
****************************************************************************
*                                                                          *
*   Initialization complete. BigchainDB is ready and waiting for events.   *
*   You can send events through the API documented at:                     *
*    - http://docs.bigchaindb.apiary.io/                                   *
*                                                                          *
*   Listening to client connections on: {:<15}                    *
*                                                                          *
****************************************************************************
"""


class Processes(object):

    def start(self):
        logger.info('Initializing BigchainDB...')

        # start the web api
        app_server = server.create_server(bigchaindb.config['server'])
        p_webapi = mp.Process(name='webapi', target=app_server.run)
        p_webapi.start()

        logger.info('starting block')
        block.start()

        logger.info(BANNER.format(bigchaindb.config['server']['bind']))
