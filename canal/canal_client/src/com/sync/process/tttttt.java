package com.sync.process;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by WangXiao on 2019/7/9.
 */
public class tttttt {
    public static void main(String[] args) throws Exception{
        SimpleDateFormat ft = new SimpleDateFormat ("yyyyMMdd");
        Date temp_now_date = ft.parse("2019-07-06 10:20:10".substring(0, 10).replaceAll("-", ""));
        System.out.println(temp_now_date);
        String aaa ="20190705";
        if(aaa.equals(ft.format(temp_now_date))){
            System.out.println("------------");
        }


    }
}
