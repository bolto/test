import logging
import threading
import time

from threading_test.managed_task import ManagedTask
from threading_test.print_something import PrintSomething

logger = logging.getLogger()


class FakeResourceThread(ManagedTask):
    def __init__(self):
        super(FakeResourceThread, self).__init__()


class FetchStopSignal(ManagedTask):
    def __init__(self, stop_flag_to_update, interval=None, duration=None, start_ts=None):
        super(FetchStopSignal, self).__init__(
            **ManagedTask.keep_used_args(start_ts=start_ts, duration=duration, interval=interval))
        self._stop_flag_to_update = stop_flag_to_update

    def execute_task(self):
        initial_flag = self._stop_flag_to_update.stop
        read_flag = False
        try:
            with open('/tmp/stop_flag', 'r') as file:
                lines = file.readlines()
                read_flag = lines[0].strip().lower() == "True".lower()
            if initial_flag != read_flag:
                logger.info("Flag changed to {}".format(read_flag))
            self._stop_flag_to_update.stop = read_flag
        except Exception as ex:
            logger.error(ex)
            pass


if __name__ == '__main__':
    threads = []
    update_threads = []
    thread_size = 2
    duration = 60
    for i in range(0, thread_size):
        printTask = PrintSomething()
        thread = threading.Thread(target=printTask.run)
        threads.append(thread)
        stop_signal_update = FetchStopSignal(stop_flag_to_update=printTask.stop_flag(), duration=duration)
        stop_update_thread = threading.Thread(target=stop_signal_update.run)
        update_threads.append(stop_update_thread)

    for t in threads:
        t.start()
    for t in update_threads:
        t.start()
    time.sleep(2)
    for t in threads:
        t.join()
    for t in update_threads:
        t.join()
