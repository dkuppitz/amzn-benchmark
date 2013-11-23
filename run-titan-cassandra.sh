#!/bin/bash

./run-titan.sh cassandra --compact -b 25000
mv /mnt/data/amzn/cassandra /mnt/data/amzn/cassandra-compact

./run-titan.sh cassandra --compact --sorted -b 25000
mv /mnt/data/amzn/cassandra /mnt/data/amzn/cassandra-sorted
