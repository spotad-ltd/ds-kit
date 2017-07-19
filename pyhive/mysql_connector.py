import mysql.connector
import logging
from utils.sql_connector.connector import Connector
from mysql.connector import errorcode

from utils.report_utils import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class MysqlConnector(Connector):
    def __init__(self, host, port, user, password, database):
        super(MysqlConnector, self).__init__(host, port)
        self.user = user
        self.password = password
        self.database = database

    def connect(self, autocommit=False):
        try:
            self.cnx = mysql.connector.connect(user=self.user, password=self.password, host=self.host, port=self.port
                                                  , database=self.database)
            self.cnx.autocommit = autocommit
            self.cursor = self.cnx.cursor()
            logger.info('Connected successfully to ' + self.database)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.error("Error with user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.error("Database does not exist")

    def commit(self):
        self.cnx.commit()
