#! /bin/bash
cd $(dirname `readlink -f $0`)
source /etc/profile
source /home/jiangcx/.bashrc
#source ../lib/msg.sh
for i in {-2..-2};do
        day=`date -d ${i}day "+%Y-%m-%d"`
        oldday=`date -d $((i-2))day "+%Y-%m-%d"`
        hour=`date -d -1hour "+%Y%m%d%H"`
        ip="10.42.4.78:8020"
        state=`/home/hadoop/hadoop-2.6.0/bin/hadoop  fs -ls hdfs://$ip/  2>&1`
        if [[ "${state}" =~ "standby" ]]; then
        ip="10.42.178.9:8020"
        fi
        echo ${ip}": is work"
        input="hdfs://"${ip}"/user/wangshuang/apps/xdata-2/user_history/dt=$day"
        # hdfs://10.42.178.9:8020/user/wangshuang/apps/xdata-2/user_history/dt=$day"
        prefix="vitemcf_"
        mail=jiangcx@tangdou.com
        outputdir=/user/jiangcx/vector/video_itemcf
        output=$outputdir/day=$day
        hadoop fs -rmr $outputdir/day=$day/experiment
#       hadoop fs -rmr $outputdir/day=$oldday
#        hadoop fs -mkdir $output

        spark-submit \
            --master yarn-cluster \
            --queue root.default \
            --class com.wifi.recommend.ItemCF_experiment \
            --conf spark.yarn.maxAppAttempts=1 \
            --conf spark.driver.maxResultSize=10g \
            --conf spark.network.timeout=1000000 \
            --conf spark.driver.memory=20g \
            --conf spark.executor.memory=10g \
            --conf spark.executor.cores=1 \
            --conf spark.executor.instances=50 \
            --conf spark.app.name="ItemCF: video_experiment $day" \
            recall-rank-1.0-SNAPSHOT.jar \
            $input $output $prefix $oldday $day

#       hadoop fs -cat $output/item/* > simlist.$day
#       size=`ls -l simlist.$day | awk '{print $5}'`
#       echo "vector size = $size"
#       if [ $size -lt 10000 ];then
#         msg "error: vitemcf simlist.$day size invalid"
#         cp itemcf.log itemcf.log.$day
#         rm simlist.$oldday
#         exit 1
#       fi
#       python import_redis.py simlist.$day
#       rm simlist.$oldday
done

hadoop fs -cat $output/experiment/* > simlist_small.$day
/data/anaconda2/envs/jcxpython3/bin/python import_redis_v1.py  simlist_small.$day