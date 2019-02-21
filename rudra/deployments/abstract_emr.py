"""Abstraction for the EMR Script."""

import abc


class AbstractEMR(metaclass=abc.ABCMeta):
    """Abstract class for the EMR Scripts."""

    @abc.abstractmethod
    def construct_job(self, input_dict):
        """Construct emr job."""
        pass

    @abc.abstractmethod
    def run_job(self, input_dict):
        """Run emr Job."""
        pass
