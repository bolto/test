import logging
import uuid

from threading_test.managed_task import ManagedTask

logger = logging.getLogger(__name__)


class PrintSomething(ManagedTask):
    def __init__(self, duration=60, interval=3, call_back_dict=None):
        super(PrintSomething, self).__init__(duration=duration, interval=interval, call_back_dict=call_back_dict)

    def execute_task(self):
        logger.info("PrintSomething execute_task : {}".format(self.id()))
