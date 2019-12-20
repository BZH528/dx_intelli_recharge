package com.switchfield;

import com.switchfield.base.SwitcherStrategy;
import com.switchfield.entity.SwitchFuncParam;
import com.sync.common.GetProperties;
import com.switchfield.switcher.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by WangXiao on 2019/4/28.
 * 外部直接使用的类
 */
public class SwitchContext {
    //Map<table,转换字段列表> 运行时初始化值  减少文件IO readtimes
    Map<String,String> judgeMap = new HashMap<>();
    //Map<table_field,转换Params> 运行时初始化值
    Map<String,SwitchFuncParam> allSwitcherWithParams = new HashMap<>();
    //一种方法只生成一个switcher对象，复用  避免对象的多次、重复生成和销毁
    Map<String,SwitcherStrategy> allSwitcherObj = new HashMap<>();
    //不存在的类路径
    Map<String,Boolean> classnotFound = new HashMap<>();

    /**
     * 相同方法名只生成一个对象
     * @param methodName 方法名
     * @return SwitcherStrategy
     */
    private SwitcherStrategy newSwitcher(String methodName){
        if (null == methodName || methodName.equals("")){
            return null;
        }
        methodName = methodName.toLowerCase();
        SwitcherStrategy resultObj = this.allSwitcherObj.get(methodName);
        methodName = methodName.toLowerCase();
        String switcherClassPath = "com.switchfield.switcher." +
                methodName.substring(0, 1).toUpperCase() + methodName.substring(1) + "Switcher";
        if (null != classnotFound.get(switcherClassPath) &&
                !classnotFound.get(switcherClassPath)){
            return null;
        }
        if(null == resultObj){
            try {
                Class<?> cls = Class.forName(switcherClassPath);
                resultObj = (SwitcherStrategy) cls.newInstance();
            }catch (Exception e){
                //e.printStackTrace();
                classnotFound.put(switcherClassPath,false);
            }
            if(null != resultObj) {
                allSwitcherObj.put(methodName, resultObj);
            }
        }
        return resultObj;
    }

    /**
     * 获取转换后的新值
     * 运行时初始化各个内部Map
     * @param tableName 表名
     * @param field 字段名
     * @param oldValue 当前值
     * @return 转换后的值
     */
    public String getNewValue(String tableName, String field, String oldValue){
        tableName = tableName.toLowerCase();
        field = field.toLowerCase();
        String newValue = oldValue;
        String switchFields = this.judgeMap.get(tableName);
        if (null == switchFields){
            String methodValue = GetProperties.hbase_table_pk.getProperty("switch_" + tableName);
            if(null == methodValue || methodValue.equals("")){
                this.judgeMap.put(tableName,"00000");
            }else{
                methodValue = methodValue.toLowerCase();
                String[] allFieldWithMethod = methodValue.split(",");
                String temp_switchFields = "";
                SwitchFuncParam switchFuncParam = null;
                //将该表的所有方法配置过一遍
                for (int i = 0; i < allFieldWithMethod.length; i++){
                    String[] temp = this.packParam(allFieldWithMethod[i]);
                    if(null == temp) {continue;}
                    if(null != this.newSwitcher(temp[1])) {
                        switchFuncParam = new SwitchFuncParam(this.newSwitcher(temp[1]),
                                temp[1], temp[2]);
                        this.allSwitcherWithParams.put(tableName + "_" + temp[0], switchFuncParam);
                        temp_switchFields = temp_switchFields + temp[0] + ",";
                    }
                }
                this.judgeMap.put(tableName,temp_switchFields);
            }
        }else if (switchFields.equals("00000")){
            return oldValue;
        }
        SwitchFuncParam sfp = this.allSwitcherWithParams.get(tableName + "_" + field);
        if(null != sfp){
            newValue = sfp.getSwitcher().switchValue(oldValue,sfp.getParams());
        }
        return newValue;
    }

    /**
     * 参数解析保护，防止配置文件中配置错误
     * @param switchParam 配置文件中的参数
     * @return 数组:[对象,方法名,方法参数]
     */
    private String[] packParam(String switchParam){
        String[] p_ret = null;
        if (null == switchParam || switchParam.equals("")){
            return p_ret;
        }
        String[] splits = switchParam.split(":");
        if (splits.length == 3){
            p_ret = splits;
        }else if (splits.length == 2){
            p_ret = new String[3];
            p_ret[0] = splits[0];
            p_ret[1] = splits[1];
            p_ret[2] = "";
        }else{
            p_ret = null;
        }
        return p_ret;

    }




}
