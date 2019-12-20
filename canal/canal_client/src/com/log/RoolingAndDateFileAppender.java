package com.log;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.helpers.CountingQuietWriter;
import org.apache.log4j.helpers.LogLog;
/**
 * Created by WangXiao on 2018/8/17.
 * 添加删除日志功能
 * 如果maxIndex>0 则只保留离当前时间最近的maxIndex个日志文件
 * 如果maxIndex<=0 则不会删除日志
 */
public class RoolingAndDateFileAppender extends RollingFileAppender{
    private String datePattern ="yyyy-MM-dd HH-mm-ss";
    private String dateStr="";//文件后面的日期
    private String expireDays ="1";//保留最近几天
    private String isCleanLog="true";
    private String maxIndex="100";
    private File rootDir;
    public void setDatePattern(String datePattern){
        if(null!=datePattern&&!"".equals(datePattern)){
            //暂时定死这个格式了 // TODO: 2018/8/17 后续修改
            this.datePattern="yyyy-MM-dd HH-mm-ss";
        }
    }
    public String getDatePattern(){
        return this.datePattern;
    }
    public void rollOver(){
        dateStr=new SimpleDateFormat(this.datePattern).format(new Date(System.currentTimeMillis()));
        File target = null;
        File file=null;
        if(qw!=null){
            long size=((CountingQuietWriter)this.qw).getCount();
        }
        //获取当天日期文件个数
        cleanLog();
        //生成新文件
        target=new File(fileName+"--"+dateStr);
        this.closeFile();
        file=new File(fileName);
        LogLog.debug("Renaming file"+file+"to"+target);
        file.renameTo(target);
        try{
            setFile(this.fileName,false,this.bufferedIO,this.bufferSize);
        }catch(IOException e){
            LogLog.error("setFile("+this.fileName+",false)call failed.",e);
        }
    }
    public synchronized void cleanLog(){
        File f=new File(fileName);
        rootDir=f.getParentFile();
        File[] listFiles = rootDir.listFiles();
        int filenum = listFiles.length;
        //如果maxIndex<=0则不需命名
        int maxfiles = Integer.parseInt(maxIndex);
        if(maxIndex != null && maxfiles > 0){
            if (filenum >= 2*maxfiles){
                binarySort(listFiles,0,filenum-1);
                for (int i = 0; i < filenum/2; i++){
                    listFiles[i].delete();
                }
            }
        }
    }
    public Boolean isExpTime(String time){
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try{
            Date logTime=format.parse(time);
            Date nowTime=format.parse(format.format(new Date()));
            //算出日志与当前日期相差几天
            int days=(int)(nowTime.getTime()-logTime.getTime())/(1000*3600*24);
            if(Math.abs(days)>=Integer.parseInt(expireDays)){
                return true;
            }else{
                return false;
            }
        }catch(Exception e){
            LogLog.error(e.toString());
            return false;
        }
    }
    /**
     * 如果当天日志达到最大设置数量，则每次删除尾号为1的日志，
     * 其他日志编号依次减去1，重命名
     * @return
     */
    public Boolean reLogNum(){
        boolean renameTo=false;
        File startFile = new File(this.fileName+'.'+dateStr+'.'+"1");
        if(startFile.exists()&&startFile.delete()){
            for(int i=2;i<=Integer.parseInt(maxIndex);i++){
                File target = new File(this.fileName+'.'+dateStr+'.'+(i-1));
                this.closeFile();
                File file = new File(this.fileName+'.'+dateStr+'.'+i);
                renameTo=file.renameTo(target);
            }
        }
        return renameTo;
    }
    public String getDateStr() {
        return dateStr;
    }
    public void setDateStr(String dateStr) {
        this.dateStr = dateStr;
    }
    public String getExpireDays() {
        return expireDays;
    }
    public void setExpireDays(String expireDays) {
        this.expireDays = expireDays;
    }
    public String getIsCleanLog() {
        return isCleanLog;
    }
    public void setIsCleanLog(String isCleanLog) {
        this.isCleanLog = isCleanLog;
    }
    public String getMaxIndex() {
        return maxIndex;
    }
    public void setMaxIndex(String maxIndex) {
        this.maxIndex = maxIndex;
    }



    public static void insertSort(int[] a) {
        for (int i = 1; i < a.length; i++) {
            int key = a[i];
            int j = i - 1;
            while (j >= 0 && a[j] > key) {
                a[j+1] = a[j];
                j--;
            }
            a[j+1] = key;
        }
    }

    public static void main(String[] args) {
        int a[] = { 5, 2, 45, 7, 2, 4, 2, 45, 7, 2, 4, 2, 45, 7, 2, 4, 23, 7,
                2, 3, 0, 43, 23, 12, 4, 1, 15, 7, 3, 8, 31 };
        insertSort(a);
        for (int i = 0; i < a.length; i++) {
            System.out.print(a[i] + " ");
        }
    }




    public int binarySerch(File[] arr, int start, int end, File value) {
        int mid = -1;
        while (start <= end) {
            mid = (start + end) / 2;
            if (arr[mid].lastModified() < value.lastModified())
                start = mid + 1;
            else if (arr[mid].lastModified() > value.lastModified())
                end = mid - 1;
            else
                break;
        }
        if (arr[mid].lastModified() < value.lastModified())
            return mid + 1;
        else if (value.lastModified() < arr[mid].lastModified())
            return mid;

        return mid + 1;
    }

    public void binarySort(File[] arr, int start, int end) {
        for (int i = start + 1; i <= end; i++) {
            File value = arr[i];
            int insertLoc = binarySerch(arr, start, i - 1, value) ;
            for (int j = i; j > insertLoc; j--) {
                arr[j] = arr[j - 1];
            }
            arr[insertLoc] = value;
        }
    }


}

