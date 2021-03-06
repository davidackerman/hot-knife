#!/bin/bash

OWN_DIR=`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$ABS_DIR/flintstone/flintstone.sh
JAR=/groups/saalfeld/home/saalfelds/spark/hot-knife/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkGenerateFaceScaleSpace
N_NODES=10

N5PATH='/nrs/flyem/data/tmp/Z1217-19m/VNC.n5'
N5DATASETINPUT='/slab-22/raw/s0'
N5GROUPOUTPUT='/slab-22/bot'
MIN='0,3328,0'
SIZE='0,-512,0'
BLOCKSIZE='1024,1024'

ARGV="\
--n5Path '$N5PATH' \
--n5DatasetInput '$N5DATASETINPUT' \
--n5GroupOutput '$N5GROUPOUTPUT' \
--min '$MIN' \
--size '$SIZE' \
--blockSize '$BLOCKSIZE'"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
