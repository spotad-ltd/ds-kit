import logging
import traceback  # stack trace upon exception
from utils.report_utils import setup_logging
from utils.mail_wrapper import MailWrapper

setup_logging()
logger = logging.getLogger(__name__)


class Connector(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.cursor = None
        self.cnx = None

    def send_query(self, query, fetchable=False, should_send_mail_on_failure=False):
        if not query:
            logger.error('Received empty query string')
        try:
            self.cursor.execute(query)
        except Exception:
            logger.error('Exception during running query: ' + str(query))
            traceback.print_exc()
            if should_send_mail_on_failure:
                mail = MailWrapper('elirans@spotad.co', 'Exception during running query: ' + query)
                mail.send_mail()
            exit(1)
        if fetchable:
            size = 10000
            print "cursor=%s" % self.cursor
            print "cursor.desc=%s" % self.cursor.description

            try:
                rows_fetched = list()
                while True:
                    rows = self.cursor.fetchmany(size)
                    #print "type(rows)=%s rows=%s" % (type(rows), rows)

                    rows_fetched += rows
                    if not rows:
                        break
                return rows_fetched
            except Exception:
                logger.error('Exception during running query: ' + query)
                traceback.print_exc()
                if should_send_mail_on_failure:
                    mail = MailWrapper('elirans@spotad.co', 'Exception during running query: ' + query)
                    mail.send_mail()
                exit(1)

    def __del__(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.cnx is not None:
            self.cnx.close()
        logger.info('Disconnected from server')

