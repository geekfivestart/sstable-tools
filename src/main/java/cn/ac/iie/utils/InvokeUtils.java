package cn.ac.iie.utils;

import java.lang.reflect.*;

/**
 * 反射调用工具类
 *
 * @author Xiang
 * @date 2016-09-20 17:10
 */
public class InvokeUtils {

    /**
     * 读取对象的private属性值
     * @param obj 某类对象
     * @param name 属性名称
     * @return 返回属性值
     * @throws NoSuchFieldException 不存在该属性异常
     * @throws IllegalAccessException 非法权限异常
     */
    public static Object readPrivate(Object obj, String name)
            throws NoSuchFieldException, IllegalAccessException {
        Field type = obj.getClass().getDeclaredField(name);
        type.setAccessible(true);
        return type.get(obj);
    }

    /**
     * 调用某对象的private方法
     * @param obj 某类对象
     * @param name 方法名称
     * @return 返回方法调用结果
     * @throws NoSuchMethodException 不存在该方法异常
     * @throws InvocationTargetException 调用目标异常
     * @throws IllegalAccessException 非法权限异常
     */
    public static Object callPrivate(Object obj, String name)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method m = obj.getClass().getDeclaredMethod(name);
        m.setAccessible(true);
        return m.invoke(obj);
    }

    /**
     * 获取泛型类型名称
     * @param clazz 某类型
     * @return 返回泛型类型名称
     */
    public static String genericSuperclass(Class clazz){
        Type t = clazz.getGenericSuperclass();
        String className = "";
        if(t instanceof Class){
            className = ((Class)t).getName();
        } else if(t instanceof ParameterizedType){
            className = ((ParameterizedType)t).getActualTypeArguments()[0].getTypeName();
        }
        return className;
    }
}
