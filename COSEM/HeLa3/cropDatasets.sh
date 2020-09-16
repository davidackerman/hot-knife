#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkCrop
N_NODES=15

datasetnames=mito

path=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5

ARGV="--n5PathToCropTo '$path' \
--datasetNameToCropTo 'er' \
--inputN5Path  '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5' \
--inputN5DatasetName '$datasetnames' \
--outputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5' \
--outputN5DatasetSuffix '_cropped'
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV 
