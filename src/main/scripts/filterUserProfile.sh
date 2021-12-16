#!/usr/bin/env bash
source ~/.bash_profile
cd /home/trainning/liujikun/vector

day=`date -d -1day "+%Y-%m-%d"`
onlyOld=0
output=/liujikun/profile_cs/day=$day/

hadoop fs -rm -r $output >/dev/null 2>&1

valid=0
for i in 1 2 3; do
  day=`date -d -${i}day "+%Y-%m-%d"`
  profile="/rec/user_log_model_online/trans_hive/pt=$day/ptype=cs"
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
--class com.wifi.recommend.FilterUserProfile \
--conf spark.driver.maxResultSize=10g \
./recall-rank-1.0-SNAPSHOT.jar \
$profile $onlyOld $output

hadoop fs -rm -r /liujikun/profile_cs/day=`date -d -7day "+%Y-%m-%d"`/
