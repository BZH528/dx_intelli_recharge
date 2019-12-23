package com.sync.process;

import com.realtimestatistics.CacheRecovery;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by WangXiao on 2018/11/23.
 * 捕获linux下kill PID关闭信号
 */
public class KillHandler implements SignalHandler {
    //kill信号是否被激活
    private boolean killSignalActived = false;
    //<线程唯一标识, 线程任务是否执行完成>
    private Map<String,Boolean> registeredThreadWorkStatus = null;
    //捕获的kill信号名称
    private final static String SIGNAL_NAME="TERM";
    //最长等待其他注册线程结束时间 秒
    private final int LONGEST_WAIT_TIME = 30;

    private static KillHandler instance = null;

    public static KillHandler getInstance(){
        if (instance == null) {
            synchronized (KillHandler.class){
                if (instance == null) {
                    instance = new KillHandler(SIGNAL_NAME);
                }
            }
        }
        return instance;
    }
    public KillHandler(String signalName){
        Signal signal = new Signal(signalName);
        Signal.handle(signal, this);
        registeredThreadWorkStatus = new HashMap<>();
    }

    public boolean getKillStatus(String thread){
        if(null == registeredThreadWorkStatus.get(thread)){
            registeredThreadWorkStatus.put(thread,false);
        }
        return registeredThreadWorkStatus.get(thread);
    }

    /**
     * 需要清理资源的线程注册
     * @param thread：线程的唯一标识字符串
     */
    public void register(String thread){
        setKillStatus(thread,false);
    }

    /**
     * 线程销毁时需要调用注销
     * @param thread:线程的唯一标识字符串
     */
    public void unregister(String thread){
        registeredThreadWorkStatus.remove(thread);
    }

    private void setKillStatus(String thread,boolean status){
        registeredThreadWorkStatus.put(thread,status);
    }

    public boolean checkProcessEnd(String thread){
        if (!killSignalActived){
            return false;
        }
        try{
            System.out.println(thread + "线程: 结束业务工作,可以进行资源清理");
            setKillStatus(thread,true);
            // TODO: 2018/12/5 sleep这种方式不是很好,后期需要结合具体业务或线程中断的方式来改进
            Thread.sleep(3600*1000);
        }catch (Exception e){}
        return true;
    }

    /**
     * 进程结束时等待注册的各个线程执行完成各自的任务
     * 最长等待时间LONGEST_WAIT_TIME 秒
     * @return true:全部线程完成自身工作  false:部分线程未完成自身工作，而且不再等待
     */
    public boolean checkRegistedThreadFinishJob(){
        int tryTimes = 0;
        while(tryTimes < LONGEST_WAIT_TIME){
            boolean allOk = true;
            for (Map.Entry<String,Boolean> ele : registeredThreadWorkStatus.entrySet()){
                allOk = allOk & ele.getValue();
            }
            if (allOk)
                return true;
            waitForAMoment();
            tryTimes++;
        }
        return false;
    }

    private void waitForAMoment(){
        try{
            Thread.sleep(1000);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void handle(Signal signal) {
        System.out.println("start cleaning.......................");
        if (signal.getName().equals("TERM")) {
            killSignalActived = true;
            if(checkRegistedThreadFinishJob()){
                System.out.println("各个注册线程正常执行完毕");
            }else{
                System.out.println("存在未执行完成的注册线程,资源清理后可能会有问题,请自查");
            }
            CacheRecovery.getInstance().save();
        }else{
            System.out.println("do not support this signal:" + signal.getName());
        }
        System.out.println("end clean.......................");
        System.exit(0);
    }
}