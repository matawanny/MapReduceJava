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
hdfs_folder_clean "source/mapreducejava/evenodd"
hdfs_folder_clean "source/mapreducejava/evenodd/output"


cd /usr/book/data/evenodd

hadoop fs -copyFromLocal evenodd.txt /user/oozie/source/mapreducejava/evenodd

inputPath='/user/oozie/source/mapreducejava/evenodd/evenodd.txt'
outputPath='/user/oozie/source/mapreducejava/evenodd/output'

hadoop jar $HBASEJAVA_JAR com.yieldbook.mortgage.hadoop.mapReduce.evenodd.MyDriver $inputPath $outputPath
