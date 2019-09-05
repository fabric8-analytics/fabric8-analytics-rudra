#!/bin/bash
# Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

set -e -x

# enable debugging & set strict error trap
sudo yum install -y zip gcc-c++ git python36-pip python36-requests httpd httpd-devel python36-devel wget git 
sudo pip install --upgrade pip
sudo python3.6 -m pip install pandas
sudo python3.6 -m pip install Cython==0.29.1
sudo python3.6 -m pip install hpfrec==0.2.2.9
sudo python3.6 -m pip install git+https://github.com/fabric8-analytics/fabric8-analytics-rudra

# Now set the PYTHONPATH
export PYTHONPATH='/home/hadoop'
