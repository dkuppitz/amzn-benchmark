#!/bin/bash

rm -rf /data/amzn/neo-graph-full

OPTS="-Xms10G -Xmx25G -XX:-HeapDumpOnOutOfMemoryError -javaagent:./lib/jamm-0.2.5.jar -cp target/Amazon-1.0-SNAPSHOT.jar:.:target/lib/*"
java $OPTS com.thinkaurelius.amazon.benchmark.Neo4jLoader \
	--path /data/amzn/neo-graph-full                  \
	-a /data/amzn/asins.txt.gz                        \
	-u /data/amzn/userids.txt.gz                      \
	-c /data/amzn/category-paths.txt.gz               \
	-t /data/amzn/titles.utf8.gz                      \
	-r /data/amzn/all.utf8.gz                         \
	-l /data/amzn/categories.utf8.gz
