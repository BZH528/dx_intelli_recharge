package com.sync.timer;

import com.sync.common.HBaseConnection;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by WangXiao on 2018/11/13.
 * 每天定时创建hbase表 目前属于特殊逻辑
 */
public class CreateNewHbaseTable implements Runnable {
    private String tableMainName = "";
    private String doExecuteHour = "12";
    private final Long waitLoopTime = 3600L;

    public CreateNewHbaseTable(String tableName, String hour){
        this.tableMainName = tableName;
        this.doExecuteHour = hour;
    }
    private void create(String suffix) {
        int trytimes = 0;
        while (trytimes < 3) {
            try {
                HBaseConnection.getInstance().creatTable(this.tableMainName + "_" + suffix, new String[]{"info"}, false);
                //因记账业务逻辑修改而必须追加写的代码-------------------------------------------------------begin
                //因记账表逻辑修改(有updeate操作)，导致历史记录更改无法入hbase(历史记账天表已删除；每天会同步到hive)
                HBaseConnection.getInstance().creatTable("zdz:history_update_account_balance" + "_" + suffix, new String[]{"info"}, false);
                //因记账业务逻辑修改而必须追加写的代码-------------------------------------------------------end
                break;
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("建表" + this.tableMainName + "_" + suffix + "异常. Error:" + e.getMessage());
            }finally {
                trytimes = trytimes + 1;
            }
        }
    }

    public void run(){
        //首先构建当天和前一天的表
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String currentTime = df.format(new Date());
        String lastDayTime = df.format(new Date().getTime() - 86400000L);
        String nextDayTime = df.format(new Date().getTime() + 86400000L);
        this.create(currentTime);
        this.create(lastDayTime);
        this.create(nextDayTime);

        while(true){
            try {
                //每天12点多构建第二天的表
                SimpleDateFormat sdf = new SimpleDateFormat("HH");
                String currentHour = sdf.format(new Date());
                if (currentHour.equals(doExecuteHour)){
                    SimpleDateFormat temp = new SimpleDateFormat("yyyyMMdd");
                    nextDayTime = temp.format(new Date().getTime() + 86400000L);
                    this.create(nextDayTime);
                }
                Thread.sleep(this.waitLoopTime*1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String currentTime = df.format(new Date());
        String lastDayTime = df.format(new Date().getTime() - 86400000L);
        String nextDayTime = df.format(new Date().getTime() + 86400000L);
        System.out.println(currentTime);
        System.out.println(lastDayTime);
        System.out.println(nextDayTime);
        SimpleDateFormat sdf = new SimpleDateFormat("HH");
        String currentHour = sdf.format(new Date());
        System.out.println(currentHour);
    }
}
