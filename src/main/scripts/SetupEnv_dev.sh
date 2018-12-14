export ENV='dev'

case $ENV in
poc)
	export JAVA_HOME=/usr/java/jdk1.8.0_131	
	export HIVE=ybrdev79
	export BASE=/usr/book
	export EMBS=/usr/book/embs
	export EXPORT=/usr/book/export
	export APP=/usr/book/app
	export REPO=/usr/book/repository
	export IMPALA=ybgdev79
	export BIGDATA_JAR=$REPO/com/yieldbook/embs/2.0.0/embs-2.0.0-shaded.jar
	export HBASEJAVA_JAR=$REPO/com/yieldbook/MapReduceJava/2.0.0/MapReduceJava-2.0.0-shaded.jar
	export T=`date "+[%Y-%m-%d %H:%M:%S]"`		
	;;
dev)
    export JAVA_HOME=/usr/java/jdk1.8.0_172
	export HIVE=ybgdldn2a
	export BASE=/data
	export EMBS=/data/embs
	export EXPORT=/export
	export APP=/home2/app
	export REPO=/home2/repository
	export IMPALA=ybgdldn2b
	export BIGDATA_JAR=$REPO/com/yieldbook/embs/2.0.0/embs-2.0.0-shaded.jar
	export HBASEJAVA_JAR=$REPO/com/yieldbook/MapReduceJava/2.0.0/MapReduceJava-2.0.0-shaded.jar
	export T=`date "+[%Y-%m-%d %H:%M:%S]"`			
	;;
esac
