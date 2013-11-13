#!/bin/bash

./run-titan.sh cassandra --compact -b 25000
mv /data/amzn/cassandra /data/amzn/cassandra-compact

./run-titan.sh cassandra --compact --sorted -b 25000
mv /data/amzn/cassandra /data/amzn/cassandra-sorted
