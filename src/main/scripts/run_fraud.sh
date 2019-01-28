START=$(date +%s)
currentUser=$(whoami)
if [ "$currentUser" != "oozie" ]
then
    echo "Must login in as oozie"
    exit 10
fi

if [ -z "$BASE" ]
then
  source $OOZIE_HOME/SetupEnv.sh
fi
source $REPO/scripts/common.sh
hdfs_folder_check "source"
hdfs_folder_check "source/mapreducejava"
hdfs_folder_clean "source/mapreducejava/fraud"
hdfs_folder_clean "source/mapreducejava/fraud/output"

export MAPREDUCEJAVA_JAR=$REPO/com/yieldbook/MapReduceJava/2.0.0/MapReduceJava-2.0.0-shaded.jar

hadoop fs -put /usr/book/app/MapReduceJava/data/fraud/fraud.txt /user/oozie/source/mapreducejava/fraud

inputPath='/user/oozie/source/mapreducejava/fraud/fraud.txt'
outputPath='/user/oozie/source/mapreducejava/fraud/output'

hadoop jar $MAPREDUCEJAVA_JAR com.yieldbook.mortgage.hadoop.mapReduce.fraud.FraudDetectionDriver $inputPath $outputPath
