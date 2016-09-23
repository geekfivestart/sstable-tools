package cn.ac.iie.drive.commands;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * 列表类型参数
 *
 * @author Xiang
 * @date 2016-09-22 18:13
 */
public class ListParam {
    public final List<String> values = Lists.newArrayList();

    private ListParam(){}

    public static ListParam fromString(String rawValue){
        ListParam listParam = new ListParam();
        if(rawValue == null || "".equals(rawValue))
            return listParam;
        if(rawValue.contains(",")){
            try {
                List<String> splitValues = Lists.newArrayList(rawValue.split(","));
                listParam.values.addAll(splitValues);
                return listParam;
            } catch (Exception ignored){
            }
        }

        if (rawValue.matches(".*\\s+.*")){
            try{
                List<String> splitValues = Lists.newArrayList(rawValue.split("\\s+"));
                listParam.values.addAll(splitValues);
                return listParam;
            } catch (Exception ignored){

            }
        }
        listParam.values.add(rawValue);
        return listParam;
    }

    public static ListParam valueOf(String rawValue){
        return fromString(rawValue);
    }
}
