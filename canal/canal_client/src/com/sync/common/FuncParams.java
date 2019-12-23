package com.sync.common;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * Created by WangXiao on 2018/12/13.
 * 启动输入参数
 * 参数1：yyyy-mm-dd
 * 参数2：hh-MM-ss
 * 参数3：模式
 */
public class FuncParams {
    private static FuncParams instance = null;
    private String[] args = null;
    private String current_reading_gmt_create_hour_time = "";

    //默认8小时误差
    private final int DEFAULT_DEVIATION_HOURS = 8;

    public static FuncParams getInstance(){
        if (instance == null) {
            synchronized (FuncParams.class){
                if (instance == null) {
                    instance = new FuncParams();
                }
            }
        }
        return instance;
    }

    public void setParams(String[] inArgs){
        if (null != args){
            System.out.println("只能被初始化一次");
        }else{
            args = inArgs;
        }
    }

    /**
     * 获取设置的位点
     * dimension: "yyyy-MM-dd HH:mm:ss"
     * @return yyyy-MM-dd HH:mm:ss
     */
    public String getStartPosition(String dimension){
        SimpleDateFormat sdf = new SimpleDateFormat(dimension);
        String start_time = sdf.format(new Date());
        String in_date = null;
        String in_time = null;
        if (args.length >= 2){
            in_date = args[0];
            in_time = args[1];
            Pattern pattern_date = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
            Pattern pattern_time = Pattern.compile("[0-9]{2}:[0-9]{2}:[0-9]{2}");
            if(!pattern_date.matcher(in_date).matches() || !pattern_time.matcher(in_time).matches()){
                System.out.println("输入日期参数格式错误，正确格式:0000-00-00 00:00:00");
            }else {
                start_time = in_date + " " + in_time;
            }
        }
        return start_time;
    }

    /**
     *
     * @return true:补录模式  false：常规模式
     * 补录模式:
     *      描述:开启一小段时间后就关闭客户端了。此种情况下不调用统计逻辑
     *      命令：bulu
     * 常规模式：(默认模式)
     *      描述:从指定位点一直执行
     *      命令：normal
     */
    public boolean checkBuLuFunc(){
        if (args.length >= 3){
            String pattern = args[2];
            if (pattern!=null && pattern.equals("bulu")){
                return true;
            }
        }
        return false;
    }

    public void setReadingPosition(String hourDate){
        current_reading_gmt_create_hour_time = hourDate;
    }
    public String getReadingPosition(){
        return current_reading_gmt_create_hour_time;
    }

    /**
     * 自动计算误差范围
     * 单位：小时数
     *
     * @return
     */
    public int getDeviationHours(){
        int deviation_hours = DEFAULT_DEVIATION_HOURS;
        if (!checkBuLuFunc()) {
            String startPos = getStartPosition("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat temp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long startMillionSeconds = 0L;
            try {
                startMillionSeconds = temp.parse(startPos).getTime();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (0L == startMillionSeconds) {
                deviation_hours = DEFAULT_DEVIATION_HOURS;
            } else if ((new Date().getTime() - startMillionSeconds) < DEFAULT_DEVIATION_HOURS * 3600 * 1000L || (new Date().getTime() - startMillionSeconds) > 500 * 3600 * 1000L) {
                deviation_hours = DEFAULT_DEVIATION_HOURS;
            } else {
                deviation_hours = (int) ((new Date().getTime() - startMillionSeconds) / 1000 / 3600);
            }
        }
        return deviation_hours;
    }

    public static void main(String[] args) {
        String[] temp = new String[]{"2018-11-30","10:11:20","bulu"};
        FuncParams.getInstance().setParams(temp);
        System.out.println(FuncParams.getInstance().getDeviationHours());
    }


}
