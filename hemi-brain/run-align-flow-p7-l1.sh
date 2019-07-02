#!/bin/bash

OWN_DIR=`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$ABS_DIR/flintstone/flintstone.sh
JAR=$PWD/hot-knife-0.0.2-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkPairAlignFlow
N_NODES=80

N5_PATH='/nrs/flyem/data/tmp/Z0115-22.n5'
N5_GROUP_INPUT='/align-6-flow'
N5_GROUP_OUTPUT='/align-7-flow'
SCALE_INDEX='1'
STEP_SIZE='256'
MAX_EPSILON='3'
SIGMA='30'

ARGV="\
--n5Path '$N5_PATH' \
--n5GroupInput '$N5_GROUP_INPUT' \
--n5GroupOutput '$N5_GROUP_OUTPUT' \
--scaleIndex '$SCALE_INDEX' \
--stepSize '$STEP_SIZE' \
--maxEpsilon '$MAX_EPSILON' \
--sigma '$SIGMA'"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
