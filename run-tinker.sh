#!/bin/bash

OPTS="-Xms10G -Xmx25G -XX:-HeapDumpOnOutOfMemoryError -javaagent:target/lib/jamm-0.2.5.jar -cp target/Amazon-1.0-SNAPSHOT.jar:.:target/lib/*"
java $OPTS com.thinkaurelius.amazon.benchmark.TinkerGraphLoader \
	-a /mnt/data/amzn/asins.txt.gz                              \
	-u /mnt/data/amzn/userids.txt.gz                            \
	-c /mnt/data/amzn/category-paths.txt.gz                     \
	-t /mnt/data/amzn/titles.utf8.gz                            \
	-r /mnt/data/amzn/all.utf8.gz                               \
	-l /mnt/data/amzn/categories.utf8.gz $@
