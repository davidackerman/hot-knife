#!/bin/bash

OWN_DIR=`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$ABS_DIR/flintstone/flintstone-lsd.sh
JAR=$PWD/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkConnectedComponents
N_NODES=10

INPUTN5PATH='/nrs/cosem/cosem/training/v0003.2/setup27.1/HeLa_Cell2_4x4x4nm/HeLa_Cell2_4x4x4nm_it625000.n5/'
OUTPUTN5PATH='/groups/cosem/cosem/ackermand/HeLa_Cell2_4x4x4nm_it625000_analysis.n5'
MASKN5PATH='/groups/cosem/cosem/data/HeLa_Cell2_4x4x4nm/HeLa_Cell2_4x4x4nm.n5/'

ARGV="\
--inputN5Path '$INPUTN5PATH' \
--outputN5Path '$OUTPUTN5PATH' \
--maskN5Path '$MASKN5PATH'"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
