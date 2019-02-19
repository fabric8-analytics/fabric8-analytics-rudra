#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Local data_store interface."""

import json
import os
import pickle

from rudra.data_store.abstract_data_store import AbstractDataStore
#  from scipy.io import loadmat
#  from rudra import logger
from ruamel.yaml import YAML


class LocalDataStore(AbstractDataStore):
    """Wrapper on local filesystem, API similar to s3DataStore."""

    def __init__(self, src_dir):
        """Create a new local filesystem instance.

        :src_dir: The root directory for local filesystem object
        """
        self.src_dir = src_dir

    def get_name(self):
        """Return name of local filesystem root dir."""
        return "Local filesytem dir: " + self.src_dir

    def read_generic_file(self, filename):
        """Read a file and return its contents."""
        with open(os.path.join(self.src_dir, filename), 'rb') as _file:
            return _file.read()

    def read_json_file(self, filename):
        """Read JSON file from the data_input source."""
        with open(os.path.join(self.src_dir, filename)) as json_fileobj:
            return json.load(json_fileobj)

    def read_yaml_file(self, filename):
        """Read Yaml file from the data_input source."""
        yaml = YAML()
        yaml_content = yaml.load(self.read_generic_file(filename))
        # convet to dict
        return json.loads(json.dumps(yaml_content))

    def read_pickle_file(self, filename):
        """Read Pickle file from the data_input source."""
        pickle_content = pickle.loads(self.read_generic_file(filename))
        return pickle_content

    #  def load_matlab_multi_matrix(self, local_filename):
    #      """Load a '.mat'file & return a dict representation.
    #
    #      :local_filename: The path of the object.
    #      :returns: A dict containing numpy matrices against the keys of the
    #                multi-matrix.
    #      """
    #      try:
    #          model_dict = loadmat(os.path.join(self.src_dir, local_filename))
    #          return model_dict
    #      except Exception as exc:
    #          logger.error("Unable to load mat file \n{}".format(str(exc)))

    def upload_file(self):
        """Upload file to a data store."""
        raise NotImplementedError

    def write_json_file(self):
        """Write json file to data store."""
        raise NotImplementedError
