package com.sync.process;

import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.shell.ExecuteShellCmd;
import com.sync.common.FuncParams;
import com.sync.timer.CreateNewHbaseTable;
import com.sync.common.GetProperties;
import com.sync.common.RenewSLSClientPos;
import com.sync.timer.ReInsertHbase;
import com.sync.timer.SummarySLSShardsData;
import org.apache.log4j.PropertyConfigurator;

/**
 * config file read
 *
 * @author sasou <admin@php-gene.com> web:http://www.php-gene.com/
 * @version 1.0.0
 */
public final class task {

	public static void main(String[] args) {
		/*
		    1、init()
		 */
		/*
			1.1 读取配置文件SysConfig.properties
		    GetProperties字节码加载时，执行静态代码块，完成配置文件，完成 GetProperties静态对象的4个属性的初始化
		       int system_debug  debug模式
		       CanalData canal  canal（连接的canal-server信息）
	           Properties hbase_table_pk SysConfig.properties属性对象
	           Map<String, TargetData> target 消费定义
		 */
		if (GetProperties.canal.destination == null) {
			System.out.println("error:canal destination is null!");
			return;
		}

		/*
		    1.2 完成log4j日志配置读取与初始化
		 */
		PropertyConfigurator.configure("log4j.properties");

		/*
		   2. 执行消费逻辑
		 */
		int num = GetProperties.canal.destination.length;
		if (num > 0) {
			for (int i = 0; i < num; i++) {
				if (!"".equals(GetProperties.canal.destination[i])) {
					String type = GetProperties.target.get(GetProperties.canal.destination[i]).type;
					switch (type) {
						case "kafka":
							new Thread(new ExecuteShellCmd("sh refresh_kerberos.sh",12*3600)).start();
							new Thread(new Kafka(GetProperties.canal.destination[i])).start();
							break;
						case "redis":
							new Thread(new Redis(GetProperties.canal.destination[i])).start();
							break;
						case "elasticsearch":
							new Thread(new ElasticSearch(GetProperties.canal.destination[i])).start();
							break;
						// 2.1 自有业务线
						case "hbase":
							// 2.1.1 执行kerberos身份认证脚本（刷新证书）
							new Thread(new ExecuteShellCmd("sh refresh_kerberos.sh",12*3600)).start();
							// 2.1.2 异常数据恢复逻辑（线程批量插入数据，失败的数据在此重新同步到hbase）
							new Thread(new ReInsertHbase()).start();
							// 2.1.3 数据同步到hbase逻辑
							new Thread(new Hbase(GetProperties.hbase_table_pk.getProperty("hbase_username"),GetProperties.hbase_table_pk.getProperty("hbase_username"),0L,true)).start();
							break;
						// 2.2 总对总业务线
						case "sls-hbase":
							//获取启动参数
							FuncParams.getInstance().setParams(args);
							//刷新kerberos证书
							new Thread(new ExecuteShellCmd("sh refresh_kerberos.sh",12*3600)).start();
							//实时创建新表
							String has_special_table_to_name = GetProperties.hbase_table_pk.getProperty("new_name_table");
							if (null != has_special_table_to_name && !has_special_table_to_name.equals("") && !has_special_table_to_name.equals("null"))
								new Thread(new CreateNewHbaseTable(has_special_table_to_name,"12")).start();
							//重定位读取SLS的位点
							RenewSLSClientPos.reset(args);
							new Thread(new ReInsertHbase()).start();
							//总对总统计
							new Thread(new SummarySLSShardsData()).start();
							//开始同步
							LogHubConfig config = new LogHubConfig(GetProperties.hbase_table_pk.getProperty("ConsumerGroup"),
									"consumer_client", GetProperties.hbase_table_pk.getProperty("region"),
									GetProperties.hbase_table_pk.getProperty("sls_project"),
									GetProperties.hbase_table_pk.getProperty("sls_logstore"),
									GetProperties.hbase_table_pk.getProperty("keyid"),
									GetProperties.hbase_table_pk.getProperty("key"),
									LogHubConfig.ConsumePosition.BEGIN_CURSOR);

							ClientWorker worker = null;
							try {
								worker = new ClientWorker(new SLS2EMRHbaseFactory(), config);
							}catch (Exception e){e.printStackTrace();}
							Thread thread = new Thread(worker);
							thread.start();
							break;
						case "show":
							new Thread(new ReInsertHbase()).start();
							new Thread(new Show()).start();
							break;
						default:
							System.out.println("error:not support type!");
							break;
					}

				}
			}
		}
		new KillHandler("TERM");
	}

}
