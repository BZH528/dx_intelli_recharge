package com.sync.common;

/**
 * Created by Administrator on 2019/11/20 0020.
 */
public class KafkaElement {
    public String bootstrapService="";
    public String topic="";
    public int retries = 0;
    public  String batchSize = "16384";
    public  int replicationFactor= 3;
    public int lingerMs = 0;
    public long bufferMemery= 33554432L;
    public String kafkaAck="all";


    public String filterTable=null;
    public String filterDatabase=null;

    public void setBootstrapService(String boot){
        this.bootstrapService = boot;
    }
    public void setTopic(String topic){
        this.topic=topic;
    }


    public void setFilterTable(String table){
        this.filterTable= table;
    }
    public void setFilterDatabase(String database){
        this.filterDatabase= database;
    }

    public void setRetries (int re){
        if(re<0){
            System.out.println("the retries must over 0  please intput correct element");
            System.exit(1);
        }else{
            this.retries=re;
        }
    }

    public void setBatchSize(String batch){
        this.batchSize = batch;
    }

    public void setReplicationFactor(int replication){
        this.replicationFactor=replication;
    }

    public void setLingerMs(int ling){
        this.lingerMs=ling;
    }

    public void setBufferMemery(long buffer){
        this.bufferMemery=buffer;
    }
    public void setKafkaAck (String ack){
        if(!ack.equals("0")&& !ack.equals("all")&& !ack.equals("1")){
            System.out.println("the kafkaAck must be one of \"all\",\"1\" or \"\0\" ");
            System.exit(1);
        }else{
            this.kafkaAck = ack;
        }
    }


}
