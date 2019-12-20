package com.switchfield.switcher;

import com.switchfield.base.SwitcherStrategy;
import com.util.encrypt.MD5Sign;

/**
 * Created by WangXiao on 2019/4/28.
 * 将原字段值内容转换为MD5签名
 */
public class Md5Switcher extends SwitcherStrategy {
    /**
     * 将旧值生成为md5签名  盐为空则加默认盐
     * 默认盐规则: 结尾取length/10 + gyjx + 源串 + wx + 开头取length/8
     * 有固定盐: gyjx + 源串 + wang + 盐
     * @param oldValue 签名源串
     * @param salt 盐
     * @return md5签名
     */
    public String switchValue(String oldValue, String salt) {
        String md5Sign = "";
        String signSrc = "gyjx" + oldValue;
        if (null != salt && !salt.equals("")) {
            signSrc = signSrc + "waNg" + salt;
        } else {
            int oldLength = oldValue.length();
            signSrc = oldValue.substring(oldLength-oldLength/10) +signSrc + "Wx" + oldValue.substring(0,oldLength/8);
        }
        try {
            md5Sign = MD5Sign.sign(signSrc, "UTF-8");
        }catch (Exception e){
            md5Sign = oldValue;
        }
        return md5Sign;
    }
}
