package com.sync.common;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Created by WangXiao on 2019/1/24.
 * 黑白名单过滤 暂时用于sls-hbase补录操作
 */
public class Filter {
    private String[] whitePattern = null;
    private String[] blackPattern = null;
    public Filter(){
        String whiteList = GetProperties.hbase_table_pk.getProperty("white_list");
        String blackList = GetProperties.hbase_table_pk.getProperty("black_list");
        if (null==whiteList || "".equals(whiteList)){
            whitePattern = new String[1];
            whitePattern[0] = ".*";
        }else{
            whitePattern = whiteList.split(",");
        }
        if (null == blackList || "".equals(blackList)){
            blackPattern = new String[1];
            blackPattern[0] = "";
        }else{
            blackPattern = blackList.split(",");
        }
        if (!checkPatternRight()){
            System.out.println("Warning: 黑白名单正则格式错误，匹配所有表");
            whitePattern = new String[1];
            whitePattern[0] = ".*";
            blackPattern = new String[1];
            blackPattern[0] = "";
        }

    }
    private boolean checkPatternRight(){
        boolean result = true;
        int whitePatternCount = whitePattern.length;
        int blackPatternCount = blackPattern.length;
        try {
            for (int i = 0; i < whitePatternCount; i++) {
                Pattern.matches(whitePattern[i], "aAbb34你好23=---?DF&>$!@DFJLF我");
            }
            for (int i = 0; i < blackPatternCount; i++) {
                Pattern.matches(blackPattern[i], "aAbb34你好23=---?DF&>$!@DFJLF我");
            }
        }catch (PatternSyntaxException e){
            System.out.println("Error: 黑白名单正则格式错误." + e.getMessage());
            result = false;
        }

        return result;
    }


    public boolean isMatch(String content){
        int whitePatternCount = whitePattern.length;
        int blackPatternCount = blackPattern.length;
        boolean isMatchWhite = false;
        boolean isMatchBlack = false;

        //在黑名单中匹配上 就是需要过滤的内容
        for (int i = 0; i < blackPatternCount; i++){
            isMatchBlack = isMatchBlack | Pattern.matches(blackPattern[i], content);
        }
        if (isMatchBlack){
            return false;
        }
        //黑名单中不匹配 然后白名单中匹配 是实际需要的内容
        for (int i = 0; i < whitePatternCount; i++){
            isMatchWhite = isMatchWhite | Pattern.matches(whitePattern[i], content);
        }
        if (isMatchWhite){
            return true;
        }
        //黑名单中不匹配 然后白名单中不匹配 过滤掉
        return false;
    }


    public static void main(String[] args) {
        Filter filter = new Filter();
        System.out.println(filter.isMatch("member_card_info_9932"));
        System.out.println(filter.isMatch("account_0000"));
        System.out.println(filter.isMatch("account_0p00"));
        System.out.println(filter.isMatch("life_number_info_"));
        System.out.println(filter.isMatch("life_number_info"));
        System.out.println(filter.isMatch("life_number_info_1022"));

    }
}
