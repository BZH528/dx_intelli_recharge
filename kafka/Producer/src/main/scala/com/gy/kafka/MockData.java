package com.gy.kafka;

/**
 * Created by Administrator on 2019/11/16 0016.
 */
public class MockData {
    public static void main(String[] args){
        int max=9,min=0;
        int index =0 ;
        String ran1="";
        String ran2="";
        String ran3="";
        String ran4="";
        Producer producer = new Producer("172.31.21.11:9092,172.31.21.12:9092,172.31.21.13:9092","hbase");


        for(int i=0 ; i<300000 ; i++){
            try{
                StringBuilder uid = new StringBuilder("208842252219");
                index = (int)(Math.random()*6);
                String.valueOf((int)(Math.random()*9));
                ran1 = String.valueOf((int)(Math.random()*9));
                ran2 = String.valueOf((int)(Math.random()*9));
                ran3 = String.valueOf((int)(Math.random()*9));
                ran4 = String.valueOf((int)(Math.random()*9));
                uid.append(ran1).append(ran2).append(ran3).append(ran4);
                System.out.println(uid.toString()+","+String.valueOf(index));
                Thread.sleep(1);
                producer.sendMessage(uid.toString()+","+String.valueOf(index));
            }catch (Exception e){e.printStackTrace();}



        }


    }
}
