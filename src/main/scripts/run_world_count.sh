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
hdfs_folder_clean "source/mapreducejava/wordcount"
hdfs_folder_clean "source/mapreducejava/wordcount/output"


cd /usr/book/data/wordcount

hadoop fs -copyFromLocal word-count.txt /user/oozie/source/mapreducejava/wordcount

inputPath='/user/oozie/source/mapreducejava/wordcount/word-count.txt'
outputPath='/user/oozie/source/mapreducejava/wordcount/output'

hadoop jar $HBASEJAVA_JAR com.yieldbook.mortgage.hadoop.mapReduce.wordcount.WordCount $inputPath $outputPath
