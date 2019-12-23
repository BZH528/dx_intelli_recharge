package com.sync.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.sync.common.ReadProperties;

/**
 * GetProperties
 * 
 * @author sasou <admin@php-gene.com> web:http://www.php-gene.com/
 * @version 1.0.0
 */

public class GetProperties {
	// debug
	public static int system_debug = 0;
	// canal（连接的canal-server信息）
	public static CanalData canal = new CanalData();
	// SysConfig.properties属性对象
	public static Properties hbase_table_pk = null;
	// target（消费定义）
	public static Map<String, TargetData> target = new HashMap<String, TargetData>();

	public static Map<String, KafkaElement> kafkaElementMap = new HashMap<>();

	static {
		// read config
		ReadProperties readProperties = new ReadProperties();
		// 读取主配置文件：SysConfig.properties
		hbase_table_pk = readProperties.readProperties();
		String tmp = "";

		// debug
		tmp = String.valueOf(hbase_table_pk.get("system.debug"));
		if (!"".equals(tmp)) {
			system_debug = Integer.parseInt(tmp);
		}

		// canal  （10.193.11.12）
		tmp = String.valueOf(hbase_table_pk.get("canal.ip"));
		if (!"".equals(tmp)) {
			canal.setIp(tmp);
		}

		// canal   (11114)
		tmp = String.valueOf(hbase_table_pk.get("canal.port"));
		if (!"".equals(tmp)) {
			canal.setPort(Integer.parseInt(tmp));
		}
		// canal (instance-yuka)
		canal.setDestination(String.valueOf(hbase_table_pk.get("canal.destination")));
		canal.setUsername(String.valueOf(hbase_table_pk.get("canal.username")));
		canal.setPassword(String.valueOf(hbase_table_pk.get("canal.password")));

		tmp = String.valueOf(hbase_table_pk.get("canal.filter"));
		if (!"".equals(tmp)) {
			canal.setFilter(tmp);
		}

		// target（根据配置的target设置）
		if (canal.destination != null) {
			int num = canal.destination.length;
			if (num > 0) {
				for (int i = 0; i < num; i++) {
					TargetData target_tmp = new TargetData();
					tmp = String.valueOf(hbase_table_pk.get(canal.destination[i] + ".target_type"));
					if(!"".equals(tmp) && tmp.toLowerCase().equals("kafka")){
						KafkaElement element = new KafkaElement();
						String ka = "";
						ka= String.valueOf(hbase_table_pk.get(canal.destination[i] + ".bootstrapService"));
						if(!"".equals(ka) &&!ka.equals("null") && ka!=null){
							element.setBootstrapService(ka);
						}
						ka= String.valueOf(hbase_table_pk.get(canal.destination[i] + ".topic"));
						if(!"".equals(ka) && !ka.equals("null") && ka!=null){
							element.setTopic(ka);
						}
						ka= String.valueOf(hbase_table_pk.get(canal.destination[i] + ".retries"));
						if(!"".equals(ka) && !ka.equals("null") && ka!=null){
							element.setRetries(Integer.valueOf(ka));
						}
						ka= String.valueOf(hbase_table_pk.get(canal.destination[i] + ".batchSize"));
						if(!"".equals(ka) && !ka.equals("null") && ka!=null){
							element.setBatchSize(ka);
						}
						ka= String.valueOf(hbase_table_pk.get(canal.destination[i] + ".maxRequestSize"));
						if(!"".equals(ka) && !ka.equals("null") && ka!=null){
							element.setReplicationFactor(Integer.valueOf(ka));
						}
						ka= String.valueOf(hbase_table_pk.get(canal.destination[i] + ".lingerMs"));
						if(!"".equals(ka) && !ka.equals("null") && ka!=null){
							element.setLingerMs(Integer.valueOf(ka));
						}
						ka= String.valueOf(hbase_table_pk.get(canal.destination[i] + ".bufferMemery"));
						if(!"".equals(ka) && !ka.equals("null") && ka!=null){
							element.setBufferMemery(Long.valueOf(ka));
						}
						ka= String.valueOf(hbase_table_pk.get(canal.destination[i] + ".kafkaAck"));
						if(!"".equals(ka) && !ka.equals("null") && ka!=null){
							element.setKafkaAck(ka);
						}
						ka= String.valueOf(hbase_table_pk.get(canal.destination[i] + ".filterTable"));
						if(!"".equals(ka) && !ka.equals("null") && ka!=null){
							element.setFilterTable(ka);
						}
						ka= String.valueOf(hbase_table_pk.get(canal.destination[i] + ".filterDatabase"));
						if(!"".equals(ka) && !ka.equals("null") && ka!=null){
							element.setFilterDatabase(ka);
						}
						kafkaElementMap.put(canal.destination[i],element);
						System.out.println("getProefafjda;fja");
						System.out.println(canal.destination[i]);
						System.out.println(kafkaElementMap.get(canal.destination[i]).bootstrapService);
					}

					if (!"".equals(tmp)) {
						target_tmp.setType(tmp);
					}
					tmp = String.valueOf(hbase_table_pk.get(canal.destination[i] + ".target_ip"));
					if (!"".equals(tmp)) {
						target_tmp.setIp(tmp);
					}

					if ("kafka".equals(target_tmp.type)) {
						target_tmp.setPort(9092);
					}
					if ("redis".equals(target_tmp.type)) {
						target_tmp.setPort(6379);
					}
					if ("elasticsearch".equals(target_tmp.type)) {
						target_tmp.setPort(9200);
					}
					tmp = String.valueOf(hbase_table_pk.get(canal.destination[i] + ".target_port"));
					System.out.println("tmp=" + tmp);
					if (!"".equals(tmp) && tmp != null && !tmp.equals("null")) {
						target_tmp.setPort(Integer.parseInt(tmp));
					}

					target.put(canal.destination[i], target_tmp);
				}
			}
		}
	}
}




