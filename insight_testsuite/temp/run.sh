#!/usr/bin/env bash

# one example of run.sh script for implementing the features using python
# the contents of this script could be replaced with similar files from any major language

# I'll execute my programs, with the input directory log_input and output the files in the directory log_output
sbt package publish-local
spark-submit --class "org.vkhristenko.insightrepo.apps.MetricsProviderSparkApp" target/scala-2.11/insight-repo_2.11-0.0.1.jar $PWD/log_input/log.txt ./log_output/hosts.txt ./log_output/resources.txt ./log_output/hours.txt ./log_output/blocked.txt ./log_output/F3-like.txt
