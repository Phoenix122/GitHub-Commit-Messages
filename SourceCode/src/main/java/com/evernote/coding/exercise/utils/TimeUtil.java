package com.evernote.coding.exercise.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Created by SuryaSelvaraj on 6/20/17.
 */
public class TimeUtil {

    public static void main(String[] args) {
        //Computing Table name
        //Current UTC TIMESTAMP in YYYYMMDD format
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar calendar = Calendar.getInstance(timeZone);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss", Locale.US);
        dateFormat.setTimeZone(timeZone);
        //System.out.println(dateFormat.format(calendar.getTime()));
        String current_date = dateFormat.format(calendar.getTime());
        System.out.println(current_date);
        System.out.println(calendar.get(Calendar.HOUR_OF_DAY));
        //current time is 00:00:00
        if(calendar.get(Calendar.HOUR_OF_DAY) == 00) {
            calendar.add(Calendar.DATE, -1);
            current_date = dateFormat.format(calendar.getTime());
            System.out.println(current_date);
        }
        //Extracting table name
        String[] tableName = current_date.split(" ");
        System.out.println(tableName[0]);

        //Computing created_at values

    }

    /**
     *
     * get current date in UTC and format it to YYYYMMDD
     * and also have the logic to check if the current UTC hour is 0th hour, then get the prev day's date
     * else use the current day's date itself
     *
     * get teh hour from current utc time and format it to look like create_at value
     *
     *
     */


   /* Calendar calendar = Calendar.getInstance(); // this would default to now
calendar.add(Calendar.DAY_OF_MONTH, -5).

    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    Calendar cal = Calendar.getInstance();
System.out.println(dateFormat.format(cal)); //2016/11/16 12:08:43*/



}
