package com.shell;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;


/**
 * Created by WangXiao on 2018/8/23.
 * shell执行脚本
 * 如果要循环调用的话请使用Thread
 */
public class ExecuteShellCmd implements Runnable{
    private final static Logger log = Logger.getLogger(ExecuteShellCmd.class);
    private String cmd = "";
    private Long repeat_delay_seconds = 0L;

    public ExecuteShellCmd(String need_repeat_cmd, long repeat_delay_seconds){
        this.cmd = need_repeat_cmd;
        this.repeat_delay_seconds = repeat_delay_seconds;
    }

    public static String callShell(String shellString) {
        StringBuffer result = null;
        try {
            Process process = Runtime.getRuntime().exec(shellString);
            int exitValue = process.waitFor();
            result = new StringBuffer();
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            while ((line = input.readLine()) != null) {
                result.append(line + "\r\n");
            }
            input.close();

            if (0 != exitValue) {
                log.error("call shell failed. error code is :" + exitValue);
            }else{
                log.info("运行shell成功");
            }
        } catch (Throwable e) {
            log.error("call shell failed. " + e);
            result = null;
        }
        return result == null ?  "shell脚本有错误" : result.toString();
    }

    public void run(){
        while(true){
            try {
                String result = callShell(this.cmd);
                log.info("shell运行结果:" + result);
                if (result.indexOf("incorrect") > -1){
                    log.error("kerberos账户信息错误");
                }
                if (result.indexOf("shell脚本有错误") > -1){
                    log.error("运行脚本异常，shell脚本有错误");
                }
                Thread.sleep(this.repeat_delay_seconds*1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
