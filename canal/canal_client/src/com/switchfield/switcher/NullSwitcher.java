package com.switchfield.switcher;

import com.switchfield.base.SwitcherStrategy;

/**
 * Created by WangXiao on 2019/4/28.
 * 将原字段值转换为空
 */
public class NullSwitcher extends SwitcherStrategy {
    /**
     * 返回字符串 null
     * @param oldValue 源串
     * @param unknownParam 本处无意义的参数
     * @return 固定值null
     */
    public String switchValue(String oldValue, String unknownParam){
        return "null";
    }
}
