#!/usr/bin/env bash
source ~/.bash_profile
cd /home/trainning/liujikun/vector
clicks="/camus/news-click/topics/news-click3/hourly/{`hadoop fs -ls /camus/news-click/topics/news-click3/hourly/*/*/*/|grep -v Found|awk '{print $8}'|tail -23|cut -d'/' -f7-|tr '\n' ,|sed 's/.$//'`}"
hour=`date -d -1hour "+%Y%m%d%H"`
output=/liujikun/vector_cs/$hour
hadoop fs -rm -r $output >/dev/null 2>&1

valid=0
for d in 1 2 3; do
  day=`date -d -${d}day "+%Y-%m-%d"`
  profile="/liujikun/profile_cs/day=$day"
  if [ `hadoop fs -ls $profile|wc -l` -gt 100 ]; then
    echo using $profile
    valid=1
    break
  else
    echo warning $profile not exist
  fi
done
if [ $valid -eq 0 ]; then
  exit 1
fi

spark-submit \
--master yarn-cluster \
--executor-memory 8g \
--queue root.trainning \
--driver-memory 8g \
--executor-cores 2 \
--num-executors 100 \
--class com.wifi.recommend.FilterUserLikes \
--conf spark.driver.maxResultSize=10g \
./recall-rank-1.0-SNAPSHOT.jar \
$profile $clicks $output

input=/liujikun/user.matrix/day=20171027/
vocab=$input/vocab
model=$input/cooccur.model.p
likes=$output/likes
minNumTags=10

spark-submit \
--master yarn-cluster \
--executor-memory 2g \
--queue root.trainning \
--driver-memory 2g \
--executor-cores 1 \
--num-executors 100 \
--class com.wifi.recommend.UserVectors \
--conf spark.driver.maxResultSize=10g \
./recall-rank-1.0-SNAPSHOT.jar \
$vocab $model $likes $minNumTags $output
