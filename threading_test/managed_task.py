import datetime
import logging
import time
import uuid

logger = logging.getLogger(__name__)


class StopFlag(object):
    def __init__(self):
        self.stop = False


class InvalidStateError(RuntimeWarning):
    pass


class InvalidValueError(RuntimeWarning):
    pass


class ManagedTask(object):
    """
    This class is used to provide a template to run a task repeated under two conditions:
        if within time window to run task:
            if stop flag is False:
                execute task
            wait for time that equals to interval before this cycle ends

    Valid time window for running task is when current time is within start time stamp + duration.

    Outside valid time window, nothing happens.
    Within valid time window, task runs repeatedly whenever stop flag is False.
    """
    DEFAULT_DURATION_SEVEN_DAYS = 7 * 24 * 60 * 60
    DEFAULT_WAIT_INTERVAL_ONE_SECOND = 1

    def __init__(self, duration=DEFAULT_DURATION_SEVEN_DAYS, start_ts=None,
                 interval=DEFAULT_WAIT_INTERVAL_ONE_SECOND, call_back_dict=None):
        """
        TODO: duration's default value of seven days is likely too long and needs review later
        :param duration (int): amount of time (in seconds) to run, defaulted to seven days
        :param start_ts (datetime): time to start running workload, defaulted to datetime.datetime.now()
        :param interval (int): time in seconds to wait before next repeated task, defaulted to one second
        :param call_back_dict (dictionary): reference to external dictionary to add task's id when task is done
        """
        self.duration = duration
        self.start_ts = start_ts or datetime.datetime.now()
        self.stop_ts = self.start_ts + datetime.timedelta(seconds=duration)
        self._interval = interval
        # initialize stop flag object, this object can be accessed externally to flip stop value
        # this value is accessible via stop_flag() method.
        self._stop_flag_object = StopFlag()
        self._id = str(uuid.uuid4())[:4]
        self._call_back_dict = call_back_dict

    def id(self):
        return self._id

    def _update_callback(self):
        if self._call_back_dict is not None:
            try:
                logger.info("Task {} done, updating callback dictionary".format(self.id()))
                # TODO: refactor to properly handle callback object type instead
                self._call_back_dict[self.id()] = self
            except Exception as ex:
                logger.error(ex)
                pass

    @staticmethod
    def keep_used_args(start_ts=None, duration=None, interval=None):
        """
        Helper method to pass variables that are not None in a dictionary.  This is convenient for child classes to
        call parent init.
        """
        args = dict()
        if interval:
            args["interval"] = interval
        if duration:
            args["duration"] = duration
        if start_ts:
            args["start_ts"] = start_ts
        return args

    def stop_flag(self):
        """
        :return: stop_flag object so external logic can flip the stop flag
        """
        return self._stop_flag_object

    def stop(self):
        """
        Set stop flag to True.  Note this flag has no impact if current time has passed valid time window.
        """
        self._stop_flag_object.stop = True

    def start(self):
        """
        Set stop flag to False.  Note this flag has no impact if current time has passed valid time window.
        """
        self._stop_flag_object.stop = False

    def is_stop(self):
        """
        :return: value of current stop flag value
        """
        return self._stop_flag_object.stop

    def within_run_window(self):
        """
        :return: is current time within the time frame that task is expected to be running
        """
        now = datetime.datetime.now()
        return self.start_ts <= now <= self.stop_ts

    def running_expected(self):
        """
        :return: is task is expected to be running at the time of calling this method
        """
        return self.within_run_window() and not self.is_stop()

    def wait_interval(self):
        """
        Sleep for the duration of interval
        """
        time.sleep(self._interval)

    def run(self):
        """
        Run execute_task if time is within valid time window and stop flag is set to False
        """
        while self.within_run_window():
            if not self.is_stop():
                self.execute_task()
            self.wait_interval()
        self._update_callback()

    def execute_task(self):
        """
        Actual task to be executed by run.  This is to be implemented by child classes.
        """
        pass
