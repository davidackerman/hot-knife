#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkIDFilter
N_NODES=10

ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/setup03/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm_it650000.n5'  \
--inputN5DatasetName 'plasma_membrane_ccDefaultVolumeFilteredSkipSmoothing' \
--idsToKeep '124300276411,50893410249,109817031117,105156007564,47495616392,41406236783,43540904621,29452202107,44174152095' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
