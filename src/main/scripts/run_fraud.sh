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


cd /usr/book/data/evenodd

hadoop fs -put fraud.txt /user/oozie/source/mapreducejava/fraud

inputPath='/user/oozie/source/mapreducejava/fraud/fraud.txt'
outputPath='/user/oozie/source/mapreducejava/fraud/output'

hadoop jar $HBASEJAVA_JAR com.yieldbook.mortgage.hadoop.mapReduce.fraud.FraudDetectionDriver $inputPath $outputPath
