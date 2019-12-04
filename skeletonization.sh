#!/bin/bash

OWN_DIR=`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$ABS_DIR/flintstone/flintstone-lsd.sh
JAR=$PWD/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkSkeletonization
N_NODES=10

INPUTN5PATH='/groups/cosem/cosem/ackermand/hela_cell3_314000_connected_components_minVolumeCutoff.n5'

ARGV="--inputN5DatasetName 'mito_cc' \
--inputN5Path '$INPUTN5PATH'"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
