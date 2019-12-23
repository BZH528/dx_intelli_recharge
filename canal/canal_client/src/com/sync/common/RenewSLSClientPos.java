package com.sync.common;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.request.ListShardRequest;
import com.aliyun.openservices.log.response.ListShardResponse;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * Created by WangXiao on 2018/11/6.
 */
public class RenewSLSClientPos {
    public static void reset(String[] args) {
        String start_time = null;
        String in_date = null;
        String in_time = null;
        if (args.length >= 2){
            in_date = args[0];
            in_time = args[1];
            System.out.println("date:" + in_date + "  time:" + in_time);
            Pattern pattern_date = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
            Pattern pattern_time = Pattern.compile("[0-9]{2}:[0-9]{2}:[0-9]{2}");
            if(!pattern_date.matcher(in_date).matches() || !pattern_time.matcher(in_time).matches()){
                System.out.println("输入日期参数格式错误，正确格式:0000-00-00 00:00:00");
                System.out.println("本次默认从上次结束时位点读取");
                return;
            }
            start_time = in_date + " " + in_time;
        }else{
            System.out.println("默认从上次结束时位点读取");
            return;
        }
        String project = GetProperties.hbase_table_pk.getProperty("sls_project");
        String logStore = GetProperties.hbase_table_pk.getProperty("sls_logstore");
        Client client = new Client(GetProperties.hbase_table_pk.getProperty("region"), GetProperties.hbase_table_pk.getProperty("keyid"), GetProperties.hbase_table_pk.getProperty("key"));
        long time_stamp = Timestamp.valueOf(start_time).getTime() / 1000;
        try {
            ListShardResponse shard_res = client.ListShard(new ListShardRequest(project, logStore));
            ArrayList<Shard> all_shards = shard_res.GetShards();
            for (Shard shard : all_shards) {
                int shardId = shard.GetShardId();
                long cursor_time = time_stamp;
                String cursor = client.GetCursor(project, logStore, shardId, cursor_time).GetCursor();
                client.UpdateCheckPoint(project, logStore, GetProperties.hbase_table_pk.getProperty("ConsumerGroup"), shardId, cursor);
            }
            //重置位点过一会儿才会生效，继续执行会被新插入的记录修改掉位点
            Thread.sleep(61000);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
