package com.switchfield.switcher;

import com.switchfield.base.SwitcherStrategy;

/**
 * Created by WangXiao on 2019/4/28.
 * 将原字段值的部分内容转换为*
 */
public class MaskSwitcher extends SwitcherStrategy {
    /**
     * 转换部分连续字符为***
     * @param oldValue 源串
     * @param continuityPositon 需要转换的连续位置.
     *                        表示格式:  开始位置-[结束位置]
     *                        表示举例: 3-9,从第3个字符到第9个字符转换为*；4-：从第4个字符开始全部转换为*
     * @return 带*的字符串
     */
    public String switchValue(String oldValue, String continuityPositon){
        String newValue = "";
        int oldLength = oldValue.length();
        if (null != continuityPositon && !continuityPositon.equals("")){
            try {
                String[] scope = continuityPositon.split("-");
                int beginPos = Integer.valueOf(scope[0]);
                if (scope.length >= 2 && beginPos <= oldLength) {
                    int endPos = Integer.valueOf(scope[1]);
                    if (endPos <= beginPos || oldLength <= endPos)
                        newValue = oldValue.substring(0, beginPos) + xingxingMask(oldLength - beginPos);
                    else
                        newValue = oldValue.substring(0, beginPos) + xingxingMask(endPos - beginPos) + oldValue.substring(endPos);
                } else {
                    newValue = oldValue.substring(0, beginPos) + xingxingMask(oldLength - beginPos);
                }
            }catch (Exception e){
                //e.printStackTrace();
                newValue = "";
            }
        }
        if (newValue.equals("")) {
            int beginPos = oldLength / 3;
            int endPos = oldLength * 2 / 3;
            newValue = oldValue.substring(0, beginPos) +
                    xingxingMask(endPos - beginPos) +
                    oldValue.substring(endPos);
        }
        return newValue;
    }
    String xingxingMask(int num){
        String xings = "";
        for (int i = 0; i < num; i++){
            xings = xings + "*";
        }
        return xings;
    }
}
