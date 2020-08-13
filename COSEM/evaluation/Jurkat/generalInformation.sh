#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkGeneralCosemObjectInformation
N_NODES=2

export RUNTIME="48:00"
BASENAME=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/Jurkat
for i in {training,refinedPredictions}
do
	ARGV="\
	--inputPairs 'er_to_plasma_membrane,er_to_mito,er_to_nucleus,er_to_ribosomes' \
	--inputN5DatasetName 'plasma_membrane,mito_membrane,mito,golgi,vesicle,MVB,er,nucleus,ribosomes' \
	--skipContactSites
	--inputN5Path '$BASENAME/${i}CC.n5' \
	--outputDirectory '$BASENAME/analysisResults/$i'
	"

	TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
done
