Amazon Graph Benchmark
======================

## Download input files

```shell
cd $DATA_DIR
wget http://snap.stanford.edu/data/amazon/all.txt.gz
wget http://snap.stanford.edu/data/amazon/categories.txt.gz
wget http://snap.stanford.edu/data/amazon/titles.txt.gz
```

## Fix charset in input files

```shell
zcat categories.txt.gz | iconv -f ISO-8859-1 -t UTF-8 | gzip -9 > categories.utf8.gz
zcat titles.txt.gz | iconv -f ISO-8859-1 -t UTF-8 | gzip -9 > titles.utf8.gz

zcat all.txt.gz | split -d -l10000000 - all.part
for f in all.part*
do
  iconv -f ISO-8859-1 -t UTF-8 -o utf8.$f $f && rm -f $f
done

cat utf8.all.part* | gzip -9 > all.utf8.gz
rm -f utf8.all.part*
```

## Create shell scripts for key data extraction

### extract-all.sh

```sh
#!/bin/bash

rm -f asins.txt.gz category-paths.txt.gz userids.txt.gz

find . -name "extract-*.sh" | grep -v "$0" | xargs -n1 -P0 /bin/bash -c
cat asin1.tmp asin2.tmp | sort -u | gzip -9 > asins.txt.gz
rm -f asin1.tmp asin2.tmp
```

### extract-asin-1.sh

```sh
#!/bin/bash
zcat all.utf8.gz | grep '^product/productId:' | awk '{print $2}' > asin1.tmp
```

### extract-asin-2.sh

```sh
#!/bin/bash
zcat titles.utf8.gz categories.txt.gz | awk '{print $1}' | grep -E '^[0-9A-Z]{10}$' > asin2.tmp
```

### extract-category-paths.sh

```sh
#!/bin/bash
zcat categories.utf8.gz | grep '^ ' | sed 's/^ *//g' | sort -u | gzip -9 > category-paths.txt.gz
```

### extract-user-ids.sh

```sh
#!/bin/bash
zcat all.utf8.gz | grep '^review/userId:' | awk '{print $2}' | sort -u | gzip -9 > userids.txt.gz
```

## Extract key data

```shell
./extract-all.sh
```
