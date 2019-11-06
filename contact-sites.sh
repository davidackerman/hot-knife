#!/bin/bash

OWN_DIR=`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$ABS_DIR/flintstone/flintstone-lsd.sh
JAR=$PWD/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkContactSites
N_NODES=10

INPUTN5PATH='/groups/cosem/cosem/ackermand/hela_cell3_314000_connected_components_minVolumeCutoff.n5'
MASKN5PATH='/groups/cosem/cosem/data/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm.n5/'

ARGV="\
--inputN5DatasetName 'plasma_membrane_cc,mito_cc,golgi_cc,vesicle_cc,MVB_cc,lysosome_cc,LD_cc,er_cc,nucleus_cc,microtubules_cc,ribosomes_cc' \
--inputN5Path '$INPUTN5PATH' \
--maskN5Path '$MASKN5PATH'"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
