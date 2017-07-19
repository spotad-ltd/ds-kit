from ConfigParser import ConfigParser

from utils.report_utils import setup_logging
from utils.sql_connector.mysql_connector import MysqlConnector
from constants import constants
import logging

setup_logging()
logger = logging.getLogger(__name__)


class EmrMysql(MysqlConnector):
    def __init__(self):
        config = ConfigParser()
        config.read(constants.CONF_PATH)
        user = config.get('mysql_credentials', 'user')
        password = config.get('mysql_credentials', 'password')
        host = config.get('mysql_credentials', 'host')
        port = config.get('mysql_credentials', 'port')
        database = config.get('mysql_credentials', 'database')
        logger.info('user=' + user
                + ' password=' + password + ' host=' + host + ' port=' + str(port) + ' database=' + database)
        super(EmrMysql, self).__init__(host, port, user, password, database)


