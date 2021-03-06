#!/bin/bash

OWN_DIR=`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$ABS_DIR/flintstone/flintstone.sh
JAR=$PWD/hot-knife-0.0.3-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkPairAlignSIFTAverage
N_NODES=20

N5_PATH='/nrs/flyem/data/tmp/Z0115-22.n5'
N5_GROUP_INPUT='/align-1'
N5_GROUP_OUTPUT='/align-2a'
SCALE_INDEX='4'
STEP_SIZE='400'
LAMBDA_FILTER='0.1'
LAMBDA_MODEL='0.01'
MAX_EPSILON='50'

ARGV="\
--n5Path '$N5_PATH' \
--n5GroupInput '$N5_GROUP_INPUT' \
--n5GroupOutput '$N5_GROUP_OUTPUT' \
--scaleIndex '$SCALE_INDEX' \
--stepSize '$STEP_SIZE' \
--lambdaFilter '$LAMBDA_FILTER' \
--lambdaModel '$LAMBDA_MODEL' \
--maxEpsilon '$MAX_EPSILON'"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV

