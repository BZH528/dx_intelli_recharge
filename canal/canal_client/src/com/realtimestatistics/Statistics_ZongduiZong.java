package com.realtimestatistics;

import com.sync.common.FuncParams;
import com.sync.common.GetProperties;
import com.sync.common.HBaseConnection;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by WangXiao on 2018/11/14.
 * 总对总实时统计信息入hbase
 */
public class Statistics_ZongduiZong {
    // <当前时间维度,<统计项,具体值>>
    private Map<String,Map<String,Long>> localCache = null;
    //HBASE的统计结果记录表
    private String realtime_statistic_table = GetProperties.hbase_table_pk.getProperty("realtime_statistic_table");
    //偏离小时数
    private final int DEVIATION_HOURS = FuncParams.getInstance().getDeviationHours();
    //sls客户端ID
    private String clientId = null;
    //每个SLS客户端的分片号
    private String shardId = null;
    //内部计次 用户清理Map 防止清理逻辑无法触发
    private int recordnum = 0;

    //历史成功怎么写？
    //启动时初始化各个值,从hbase记录中取数据
    private Long getInitValue(String dstTime, String column){
        Long result = 0L;
        String rowkey = dstTime+"_"+ clientId + "_" + shardId;
        try {
            Result tt = HBaseConnection.getInstance().getData(realtime_statistic_table, rowkey, "info", column);
            Cell[] cc = tt.rawCells();
            if (cc.length != 0) {
                result = Long.valueOf(new String(CellUtil.cloneValue(cc[0])));
            }
        }catch (Exception e){
            System.out.println(column + "获取初始值失败:" + e.getMessage());
            e.printStackTrace();
            result =0L;
        }
        return result;
    }

    /**
     * 从hbase获取数据各个统计值
     * 用于关闭客户端然后重启
     * @param dstTime
     * @param clientId
     * @param shardId
     */
    public Statistics_ZongduiZong(String dstTime, String clientId,String shardId){
        this.clientId = clientId;
        this.shardId = shardId;
        ////初始化统计值
        localCache = new HashMap<String,Map<String, Long>>();
        Map<String,Long> kv = new HashMap<String, Long>();
        //开卡总数量
        kv.put("mcd_count",getInitValue(dstTime,"mcd_count"));
        //开卡成功数
        kv.put("mcd_count_s",getInitValue(dstTime,"mcd_count_s"));
        //获取流量总数
        kv.put("gfd_flow_sum",getInitValue(dstTime,"gfd_flow_sum"));
        //兑换总订单数
        kv.put("efd_order_sum",getInitValue(dstTime,"efd_order_sum"));
        //兑换成功订单数
        kv.put("efd_order_sum_s",getInitValue(dstTime,"efd_order_sum_s"));
        //兑换失败订单数
        kv.put("efd_order_sum_f",getInitValue(dstTime,"efd_order_sum_f"));
        //兑换流量的总数
        kv.put("efd_flow_sum",getInitValue(dstTime,"efd_flow_sum"));
        //兑换成功流量的总数
        kv.put("efd_flow_sum_s",getInitValue(dstTime,"efd_flow_sum_s"));
        //兑换失败流量的总数
        kv.put("efd_flow_sum_f",getInitValue(dstTime,"efd_flow_sum_f"));
        //移动中奖人数
        kv.put("luckier_chinamobile",getInitValue(dstTime,"luckier_chinamobile"));
        //支付宝中奖人数
        kv.put("luckier_alipay",getInitValue(dstTime,"luckier_alipay"));
        localCache.put(dstTime,kv);
    }
    public void initDstTimeCache(String dstTime){
        if (null == localCache.get(dstTime)){
            Map<String,Long> kv = new HashMap<String, Long>();
            //开卡数量
            kv.put("mcd_count",0L);
            //开卡成功数
            kv.put("mcd_count_s",0L);
            //获取流量总数
            kv.put("gfd_flow_sum",0L);
            //兑换总订单数
            kv.put("efd_order_sum",0L);
            //兑换成功订单数
            kv.put("efd_order_sum_s",0L);
            //兑换失败订单数
            kv.put("efd_order_sum_f",0L);
            //兑换流量的总数
            kv.put("efd_flow_sum",0L);
            //兑换成功流量的总数
            kv.put("efd_flow_sum_s",0L);
            //兑换失败流量的总数
            kv.put("efd_flow_sum_f",0L);
            //移动中奖人数
            kv.put("luckier_chinamobile",0L);
            //支付宝中奖人数
            kv.put("luckier_alipay",0L);
            localCache.put(dstTime,kv);
        }
    }
    public void resetCache(){
        if (localCache.size() > DEVIATION_HOURS)
            for (Iterator<Map.Entry<String,Map<String, Long>>> it = localCache.entrySet().iterator(); it.hasNext();){
                Map.Entry<String, Map<String, Long>> item = it.next();
                if (!deviationJudge(item.getKey())){
                    it.remove();
                }
            }
    }

    /**
     * 时间偏离判断
     * 时间点是否落在允许的时间范围内
     * @param hourTime
     * @return true:在允许范围内  false：不在允许范围内
     */
    private boolean deviationJudge(String hourTime){
        boolean result = true;
        SimpleDateFormat temp = new SimpleDateFormat("yyyy-MM-dd HH");
        try{
            if (new Date().getTime() - temp.parse(hourTime).getTime() > DEVIATION_HOURS*3600*1000L){
                result = false;
            }
        }catch (Exception e){e.printStackTrace();}
        return result;
    }
    public Put compute(String tableName, String clientId, String shardId, Map<String,String> kv){
        //meber_card_info   update  mci_status=0 && mci_zfb_id!=-1  +1   : key_field1_value=mci_status mci_zfb_id
        //account_balance   insert  ab_trade_type=1&&ab_trade_source=1  : key_field1_value=ab_trade_type  key_field2_value=ab_trade_source
        //tradeinfo
        //        1、insert 总数+1， 总流量+trade_cost  （考虑update的情况）
        //        2、update trade_status=0成功数量+1，trade_status=3失败数量+1；(update前后该字段比较trade_status)
        //                  trade_status=0成功流量+trade_cost，trade_status=3失败流量+trade_cost；

        //先判断时间点 以小时为维度
//        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH");
//        String currentTime = df.format(new Date());
        String gmt_create = kv.get("gmt_create");
        if (gmt_create == null || gmt_create.equals("")){
            System.out.println("Error: table " + tableName + " gmt_create字段空，非法！");
            return null;
        }
        String processType = kv.get("_event_");
        //"yyyy-MM-dd HH"
        gmt_create = gmt_create.substring(0,13);
        //在误差范围的记录继续统计，不在误差范围内的数据略过
        if (!deviationJudge(gmt_create)){
            this.resetCache();
            return null;
        }else{
            initDstTimeCache(gmt_create);
            //防止上面的if触发不了
            if (this.recordnum++ > 100000){
                this.recordnum = 0;
                this.resetCache();
            }
        }
        //记录下当前读取到的业务时间戳. 当前是做统计的时间点来用
        FuncParams.getInstance().setReadingPosition(gmt_create);
        if (tableName.equals("member_card_info")){
            //开卡成功人数
            if(processType.equals("row_update")) {
                String mci_status = kv.get("mci_status") + "";
                if (mci_status.equals("0")){}
                String now_mci_zfb_id = kv.get("mci_zfb_id") + "";
                String old_mci_zfb_id = kv.get("_old_mci_zfb_id") + "";
                if ((!now_mci_zfb_id.equals(old_mci_zfb_id)) && (!now_mci_zfb_id.equals("-1")) && mci_status.equals("0")) {
                    this.sum(gmt_create, "mcd_count_s", 1L);
                }
            }else if(processType.equals("row_insert")){
                this.sum(gmt_create, "mcd_count", 1L);
            }else{
                //do nothing
            }
        }else if (tableName.equals("account_balance") && processType.equals("row_insert")){
            if ("1".equals("" + kv.get("ab_trade_type")) && "1".equals("" + kv.get("ab_trade_source"))){
                this.sum(gmt_create,"gfd_flow_sum",Long.valueOf(kv.get("ab_trade_amount_real").split("\\.")[0]));
            }
        }else if (tableName.equals("tradeinfo")){
            if (processType.equals("row_update")){
                String old_trade_status = kv.get("_old_trade_status");
                String new_trade_status = kv.get("trade_status");
                //1或2--->0
                if ((old_trade_status.equals("1") || old_trade_status.equals("2"))
                        && new_trade_status.equals("0")){
                    this.sum(gmt_create,"efd_order_sum_s",1L);
                    this.sum(gmt_create,"efd_flow_sum_s",Long.valueOf(kv.get("trade_cost").split("\\.")[0]));
                }else if ((old_trade_status.equals("1") || old_trade_status.equals("2"))
                        && new_trade_status.equals("3")){
                    //1或2----->3
                    this.sum(gmt_create,"efd_order_sum_f",1L);
                    this.sum(gmt_create,"efd_flow_sum_f",Long.valueOf(kv.get("trade_cost").split("\\.")[0]));
                }else if(!old_trade_status.equals(new_trade_status) && (old_trade_status.equals("3") || old_trade_status.equals("0"))){
                    System.out.println("告警:" + kv.get("trade_id") + "---trade_status由" + old_trade_status + "改为" + new_trade_status);
                }else{
                    //历史遗留暂时不处理
                    //2-->3   3--->2
                }
            }else{
                //insert的情况
                this.sum(gmt_create,"efd_order_sum",1L);
                this.sum(gmt_create,"efd_flow_sum", Long.valueOf(kv.get("trade_cost").split("\\.")[0]));
                String trade_status = kv.get("trade_status");
                if (trade_status.equals("0")){
                    this.sum(gmt_create,"efd_order_sum_s",1L);
                    this.sum(gmt_create,"efd_flow_sum_s",Long.valueOf(kv.get("trade_cost").split("\\.")[0]));
                }else if (trade_status.equals("3")){
                    this.sum(gmt_create,"efd_order_sum_f",1L);
                    this.sum(gmt_create,"efd_flow_sum_f",Long.valueOf(kv.get("trade_cost").split("\\.")[0]));
                }else{
                    //do nothing
                }
            }
        }else if(tableName.equals("trade_record") && processType.equals("row_insert")){
            //每周五中奖者个数统计
            String luckerFrom = kv.get("si_supplier_id");
            if (luckerFrom.equals("chinamobile")){
                this.sum(gmt_create,"luckier_chinamobile",1L);
            }else if (luckerFrom.equals("alipay")){
                this.sum(gmt_create,"luckier_alipay",1L);
            }else{
                //do nothing
            }
        }else{
            //System.out.println("不支持该表" + tableName + "的统计逻辑!");
            return null;
        }
        return packPut(gmt_create, clientId, shardId);
    }
    public Put packPut(String dstTime, String clientId, String shardId){
        Put oneRecord = new Put(Bytes.toBytes(dstTime+"_"+ clientId + "_" + shardId));
        for (Map.Entry<String,Long> ele : this.localCache.get(dstTime).entrySet()){
            oneRecord.addColumn(Bytes.toBytes("info"),Bytes.toBytes(ele.getKey()),Bytes.toBytes(ele.getValue().toString()));
        }
        //使用hbase作为存储时会用到这个字段
        oneRecord.addColumn(Bytes.toBytes("info"),Bytes.toBytes("statis_hour"),Bytes.toBytes(dstTime));
        return oneRecord;
    }
    private void sum(String currentTime, String cacheKey, long value){
        long temp = localCache.get(currentTime).get(cacheKey) + value;
        localCache.get(currentTime).put(cacheKey,temp);
    }
    public long getValue(String currentTime,String key){
        return this.localCache.get(currentTime).get(key);
    }

    public static void main(String[] args) {
        Map<String,Map<String,Long>> mapCache = new HashMap<>();
        Map<String,Long> temp1 = new HashMap<>();
        temp1.put("cost",10L);
        temp1.put("save",100L);
        mapCache.put("2018-10-10",temp1);
        Map<String,Long> temp2 = new HashMap<>();
        temp2.put("cost",20L);
        temp2.put("save",300L);
        mapCache.put("2018-10-12",temp2);
        Map<String,Long> temp3 = new HashMap<>();
        temp3.put("cost",30L);
        temp3.put("save",300L);
        mapCache.put("2018-10-13",temp3);
        Map<String,Long> temp4 = new HashMap<>();
        temp4.put("cost",40L);
        temp4.put("save",400L);
        mapCache.put("2018-10-14",temp4);

        System.out.println("初始值-----------------------");
        for (String key : mapCache.keySet()){
            String sout = "main key=" + key + "\n";
            for (Map.Entry<String,Long> ele : mapCache.get(key).entrySet()){
                sout = sout + "ele.key=" + ele.getKey() + ",ele.value=" + ele.getValue();
            }
            System.out.println(sout);
        }


        //测试一下行不行
        String currentTime = "2018-10-14";
        for (Iterator<Map.Entry<String,Map<String, Long>>> it = mapCache.entrySet().iterator(); it.hasNext();){
            Map.Entry<String, Map<String, Long>> item = it.next();
            if (!item.getKey().equals(currentTime)){
                it.remove();
        }
        }

        System.out.println("结束值-----------------------");
        for (String key : mapCache.keySet()){
            String sout = "main key=" + key + "\n";
            for (Map.Entry<String,Long> ele : mapCache.get(key).entrySet()){
                sout = sout + "ele.key=" + ele.getKey() + ",ele.value=" + ele.getValue();
            }
            System.out.println(sout);
        }


    }

}
