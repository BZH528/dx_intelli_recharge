package com.switchfield.entity;

import com.switchfield.base.SwitcherStrategy;

/**
 * Created by WangXiao on 2019/4/28.
 * 转换方法以及相应参数的存储结构体
 * 解析配置参数时使用
 * 配置文件中的参数配置格式为: switch_表名=字段名:方法名:方法参数,...
 */
public class SwitchFuncParam {
    SwitcherStrategy switcher = null;
    String methodName = null;
    String params = null;

    public SwitchFuncParam(SwitcherStrategy switcher, String methodName, String params){
        this.switcher = switcher;
        this.methodName = methodName;
        this.params = params;
    }

    public SwitcherStrategy getSwitcher() {
        return switcher;
    }

    public void setSwitcher(SwitcherStrategy switcher) {
        this.switcher = switcher;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }



}
