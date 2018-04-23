package com.ijunhai.common;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Admin on 2017-08-31.
 */
public class TimeUtil {
    public static final int SECOND=1;
    public static final int MILLISECOND=0;

    public static String time2DateString(String formatStr,long timestamp,int timeUnit){
        DateFormat format = new SimpleDateFormat(formatStr);
        Date currentTime =null;
        switch (timeUnit){
            case MILLISECOND:
                currentTime=new Date(timestamp);
                break;
            case SECOND:
                currentTime=new Date(timestamp*1000);
                break;
        }
        return format.format(currentTime);
    }

    public static Long dateString2Time(String formatStr,String dateString,int timeUnit) throws ParseException {
        DateFormat format = new SimpleDateFormat(formatStr);
        Date currentTime=format.parse(dateString);
        Long timestamp=currentTime.getTime();
        switch (timeUnit){
            case MILLISECOND:
                break;
            case SECOND:
                timestamp=timestamp/1000;
                break;
        }
        return timestamp;
    }
    public static java.sql.Date dataString2SqlDate(String formatStr,String dateString) throws ParseException {
        Long timestamp=dateString2Time(formatStr,dateString,MILLISECOND);
        java.sql.Date currentTime =null;
        return new java.sql.Date(timestamp);
    }

    public static java.sql.Date time2SqlDate(long timestamp,int timeUnit){
        java.sql.Date currentTime =null;
        switch (timeUnit){
            case MILLISECOND:
                currentTime=new java.sql.Date(timestamp);
                break;
            case SECOND:
                currentTime=new java.sql.Date(timestamp*1000);
                break;
        }
        return currentTime;
    }
    public static java.sql.Date getDefaultSqlDate() throws ParseException {
        return dataString2SqlDate("yyyy-MM-dd HH:mm:ss","1970-07-01 00:00:00");
    }
    public static Integer getTimeStamp(){
        Date date=new Date();
        return (int) date.getTime()/1000;
    }
}
