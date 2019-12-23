package com.switchfield.base;

/**
 * Created by WangXiao on 2019/4/28.
 * 字段值转换方法基类
 */
public abstract class SwitcherStrategy {
    public abstract String switchValue(String oldValue, String unknownParam);
}
