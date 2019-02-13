#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Abstract class for data store interactions."""

import abc


class AbstractDataStore(metaclass=abc.ABCMeta):
    """Abstract class to dictate the behaviour of a data store."""

    @abc.abstractmethod
    def get_name(self):
        """Get name of bucket or root fs directory."""
        pass

    @abc.abstractmethod
    def read_json_file(self):
        """Read JSON file from the data source."""
        pass

    @abc.abstractmethod
    def read_generic_file(self):
        """Read a file and return its contents."""
        pass

    @abc.abstractmethod
    def read_pickle_file(self, filename):
        """Read Pickle file from data store."""
        pass

    @abc.abstractmethod
    def read_yaml_file(self, filename):
        """Read Pickle file from data store."""
        pass

    @abc.abstractmethod
    def upload_file(self, src, target):
        """Upload file into data store."""
        pass

    @abc.abstractmethod
    def write_json_file(self, filename, contents):
        """Write JSON file into data store."""
        pass
