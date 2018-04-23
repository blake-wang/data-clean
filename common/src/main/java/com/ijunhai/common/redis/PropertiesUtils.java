package com.ijunhai.common.redis;

import java.util.Properties;

public class PropertiesUtils {

    private static Properties properties=new Properties();

    static{
        try {
            properties.load(PropertiesUtils.class.getClassLoader().getResourceAsStream("clean.properties"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static String get(String key){
        return (String) properties.get(key);
    }

    public static Integer getInt(String key){
        try {
            return Integer.parseInt(get(key));
        } catch (Exception ex) {
            return null;
        }
    }
    public static int getInt(String key, int defaultValue){
        try {
            return Integer.parseInt(get(key));
        } catch (Exception ex) {
        }
        return defaultValue;
    }

    public static boolean getBoolean(String key, boolean defaultValue){
        try {
            return Boolean.parseBoolean(get(key));
        } catch (Exception ex) {
        }
        return defaultValue;
    }


    public static Long getLong(String key){
        try {
            return Long.parseLong(get(key));
        } catch (Exception ex) {}
        return null;
    }

    public static long getLong(String key, long defaultValue){
        try {
            return Long.parseLong(get(key));
        } catch (Exception ex) {}
        return defaultValue;
    }

    public static Object get(String key, Object defaultValue){
        return properties.getOrDefault(key, defaultValue);
    }


}