#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkContactSites
N_NODES=2

export RUNTIME="48:00"
BASENAME=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/Macrophage
for i in {trainingCC.n5,refinedPredictionsCC.n5}
do
	ARGV="\
	--inputPairs 'er_to_plasma_membrane,er_to_mito,er_to_nucleus,er_to_ribosomes' \
	--contactDistance 10 \
	--inputN5Path '$BASENAME/$i' \
	--minimumVolumeCutoff 0 \
	--skipGeneratingContactBoundaries
	"

	TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
done
