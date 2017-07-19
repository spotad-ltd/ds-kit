from pyhive import presto, hive, exc
from utils.mail_wrapper import *
import traceback  # stack trace upon exception
import logging
import sys

# global
from utils.sql_connector.connector import Connector

setup_logging()
logger = logging.getLogger(__name__)


class PyhiveQuery(Connector):
    def __init__(self, server_type, host, port):
        super(PyhiveQuery, self).__init__(host, port)
        self.server_type = server_type

    def connect(self):
        try:
            if self.server_type == 'presto':
                self.cursor = presto.connect(self.host, port=self.port).cursor()
                logger.info('Connected to presto server successfully')
            elif self.server_type == 'hive':
                self.cursor = hive.connect(self.host, port=self.port).cursor()
                logger.info('Connected to hive server successfully')
            else:
                raise Exception('Server type: %s not supported.' % str(self.server_type))
        except Exception:
            logger.error('ERROR: Can not connect to server')
            traceback.print_exc()
            sys.exit(1)

    def __del__(self):
        self.cursor.close()
        logger.info('Disconnected from %s server' % self.server_type)

