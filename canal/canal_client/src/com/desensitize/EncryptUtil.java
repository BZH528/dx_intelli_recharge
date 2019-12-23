package com.desensitize;

/**
 * Created by WangXiao on 2018/9/5.
 * desensitize data uitl-class
 * to cover real algorithm, use other way to name methods
 */
public class EncryptUtil {
    static {
        System.loadLibrary("encrypt");
    }
    public native static String doEncryptData1(String data);
    public native static String doEncryptData2(String data);
    public native static String doEncryptData3(String data);

    public static void main(String[] args) {
        System.out.println(EncryptUtil.doEncryptData1("我"));
    }
}
