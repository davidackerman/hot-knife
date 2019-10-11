#!/bin/bash

OWN_DIR=`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$ABS_DIR/flintstone/flintstone-lsd.sh
JAR=$PWD/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkConnectedComponents
N_NODES=2

INPUTN5PATH='/nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5'
OUTPUTN5PATH='/groups/cosem/cosem/ackermand/test_2.n5'
#INPUTN5PATH='/groups/cosem/cosem/ackermand/hela_cell3_314000_crop.n5'
#OUTPUTN5PATH='/groups/cosem/cosem/ackermand/junk.n5'
MASKN5PATH='/groups/cosem/cosem/data/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm.n5/'

ARGV="\
--inputN5Group 'LD' \
--inputN5Path '$INPUTN5PATH' \
--outputN5Path '$OUTPUTN5PATH' \
--maskN5Path '$MASKN5PATH'"

export RUNTIME="24:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
