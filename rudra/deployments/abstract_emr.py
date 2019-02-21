"""Abstraction for the EMR Script."""

import abc


class AbstractEMR(metaclass=abc.ABCMeta):
    """Abstract class for the EMR Scripts."""

    @abc.abstractmethod
    def submit_job(self, input_dict):
        """Submit emr job."""
        pass

    @abc.abstractmethod
    def run_job(self):
        """Run emr Job."""
        pass
