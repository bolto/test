import datetime
import logging
import threading
import time
import uuid

from threading_test.managed_task import ManagedTask, InvalidValueError
from threading_test.print_something import PrintSomething

logger = logging.getLogger(__name__)


class TargetActiveTaskCount(object):
    def __init__(self):
        self._target_count = 0

    def set_target(self, count):
        if count < 0:
            raise InvalidValueError(
                "Target count of threads can not be less than zero.  Supplied count: {}".format(count))
        self._target_count = count

    def count(self):
        return self._target_count


class DynamicActiveTasks(ManagedTask):
    def __init__(self, start_ts=None, duration=None, interval=None):
        super(DynamicActiveTasks, self).__init__(
            **ManagedTask.keep_used_args(start_ts=start_ts, duration=duration, interval=interval))
        self._name = str(uuid.uuid4())[:4]
        self._target_count_object = TargetActiveTaskCount()
        self.threads = dict()
        self.thread_objects = dict()
        self._completed_tasks = dict()

    def update_target_count(self):
        read_count = None
        try:
            with open('/tmp/target_count', 'r') as file:
                lines = file.readlines()
                read_count = int(lines[0].strip().lower())
            if read_count and read_count > 0 and read_count != self._target_count_object.count():
                logger.info("Target count changed to {}".format(read_count))
                self._target_count_object.set_target(read_count)
        except Exception as ex:
            logger.error(ex)
            pass

    def count(self):
        return len(self.threads.items())

    def remove_thread(self, key):
        try:
            thread = self.threads[key]
            thread_object = self.thread_objects[key]
            # stop should also remove resources
            logger.info("stopping thread {}".format(key))
            # TODO: refactor so task is not terminated via hacky stop_ts modification
            thread_object.stop_ts = datetime.datetime.now()
            thread_object.stop()
            # now the callback should take care of removal on next _completed_task purge
        except Exception as ex:
            logger.error(ex)
            pass

    def remove_threads(self):
        threads_to_remove = self.count() - self._target_count_object.count()
        if threads_to_remove <= 0:
            logger.warning("Number of threads to remove is ignore because value is {}".format(threads_to_remove))
            return
        try:
            logger.info("Threads to remove: {}".format(threads_to_remove))
            for _ in range(threads_to_remove):
                for key in self.threads.keys():
                    self.remove_thread(key)
                    break
        except Exception as ex:
            logger.error(ex)
            pass

    def add_threads(self):
        threads_to_add = self._target_count_object.count() - self.count()
        if threads_to_add <= 0:
            logger.warning("Number of threads to remove is ignore because value is {}".format(threads_to_add))
            return
        logger.info("Threads to add: {}".format(threads_to_add))
        for _ in range(0, threads_to_add):
            task = PrintSomething(call_back_dict=self._completed_tasks)
            logger.info("Adding thread {}".format(task.id()))
            thread = threading.Thread(target=task.run)
            thread.start()
            self.threads[task.id()] = thread
            self.thread_objects[task.id()] = task

    def update_threads(self):
        # monitor and adjust thread count dynamically
        keys_to_delete = []
        for key in self._completed_tasks.keys():
            keys_to_delete.append(key)
        for key in keys_to_delete:
            logger.info("removing completed thread {}".format(key))
            del self._completed_tasks[key]
            del self.threads[key]
            del self.thread_objects[key]

        if self.count() < self._target_count_object.count():
            # increase counts
            self.add_threads()
        elif self.count() > self._target_count_object.count():
            # decrease counts
            self.remove_threads()

    def execute_task(self):
        self.update_target_count()
        self.update_threads()


if __name__ == '__main__':
    threads = []
    thread_size = 1
    duration = 300
    for i in range(thread_size):
        task = DynamicActiveTasks(duration=duration)
        thread = threading.Thread(target=task.run)
        threads.append(thread)

    for t in threads:
        t.start()
    time.sleep(2)
    for t in threads:
        t.join()
