package com.util.encrypt;

/**
 * Created by WangXiao on 2019/4/29.
 */
import java.security.MessageDigest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MD5Sign {
    public static final int SORT_DISABLE = 0;
    public static final int SORT_ASC = 1;
    public static final int SORT_DESC = -1;
    private static Log log = LogFactory.getLog(MD5Sign.class);

    public static String sign(String sText, String sCharsetName)
            throws Exception {
        String s = null;
        char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                'A', 'B', 'C', 'D', 'E', 'F'};

        sCharsetName = (sCharsetName == null) || (sCharsetName.equals("")) ? "UTF-8" :
                sCharsetName;
        byte[] source = sText.getBytes(sCharsetName);

        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(source);
        byte[] tmp = md.digest();

        char[] str = new char[32];
        int k = 0;
        for (int i = 0; i < 16; i++) {
            byte byte0 = tmp[i];
            str[(k++)] = hexDigits[(byte0 >>> 4 & 0xF)];
            str[(k++)] = hexDigits[(byte0 & 0xF)];
        }
        s = new String(str);
        return s.toLowerCase();
    }
}