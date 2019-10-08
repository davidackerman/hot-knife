#!/bin/bash

OWN_DIR=`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$ABS_DIR/flintstone/flintstone-lsd.sh
JAR=$PWD/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkConnectedComponents
N_NODES=5

INPUTN5PATH='/groups/cosem/cosem/ackermand/hela_cell3_314000_crop.n5'
INPUTN5GROUP='mito'
OUTPUTN5PATH='/groups/cosem/cosem/ackermand/hela_cell3_314000_crop.n5'
OUTPUTN5GROUP='mito_cc'
MASKN5PATH='/groups/cosem/cosem/data/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm.n5/'

ARGV="\
--inputN5Path '$INPUTN5PATH' \
--inputN5Group '$INPUTN5GROUP' \
--outputN5Path '$OUTPUTN5PATH' \
--outputN5Group '$OUTPUTN5GROUP' \
--maskN5Path '$MASKN5PATH'"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
