package com.wyd.log.util;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * <p>
 * Title: 日期基类
 * </p>
 * <p>
 * Description: 一些日期相关操作
 * </p>
 */
public class DateUtil {
    // 定义默认时区
//    public final static String DEFAULT_ZONE_ID = "Asia/Hong_Kong";
    // 定义比较日期的类型
    public final static int              ERROR                  = -1000;
    public final static int              ON_DAY                 = 1;
    public final static int              ON_MONTH               = 2;
    public final static int              ON_YEAR                = 3;
    public final static int              ON_HOUR                = 4;
    public final static int              ON_MINUTE              = 5;
    public final static int              ON_SECOND              = 6;
    public final static int              ON_SYSTEM              = 7;
    // 定义日期常量
    public final static int              YEAR_MONTHS            = 12;
    public final static int              DAY_HOURS              = 24;
    public final static int              HOUR_MINUTES           = 60;
    public final static int              MINUTE_SECONDS         = 60;
    // 一天=86400000(24*60*60*1000)毫秒(MSEL)millisecond
    public final static long             DAY_MSELS              = 86400000;
    // 定义格式化日期的模板
    public final static String           PATTERN_SP1            = ":";                                        // 分隔符号
    public final static String           PATTERN_DDHHMM         = "dd:hh:mm";                                 // 天：小时：分钟
    
    public final static String 			 YYYY_MM_DD_HH_MM_SS 	= "yyyy-MM-dd HH:mm:ss";
    public final static String			 YYYY_MM_DD         	= "yyyy-MM-dd";
    public final static String			 YYYY_MM_DD_         	= "yyyy_MM_dd";
    public final static String			 YYYYMMDD         		= "yyyyMMdd";
    public final static String			 YYYY_MM_DD_HH         	= "yyyy-MM-dd_HH";
    public final static String			 YYYY_MM                = "yyyy-MM";
    public final static String			 MM_DD					= "MM-dd";
    public final static String			 HH						= "HH";
    // 存放不同的日期模版格式的sdf的map
    private static Map<String, ThreadLocal<SimpleDateFormat>> sdfMap = new HashMap<String, ThreadLocal<SimpleDateFormat>>();
    
    /** 锁对象 */
    private static final Object lockObj = new Object();
    
    /**
     * 返回一个ThreadLocal的sdf, 每个线程只会new一次sdf
     * @author jian_xie
     */
    private static SimpleDateFormat getSdf(final String pattern){
    	ThreadLocal<SimpleDateFormat> t1 = sdfMap.get(pattern);
    	// 此处的双重判断和同步是为了防止sdfmap这个单例被多次重复的sdf
    	if(t1 == null){
    		synchronized (lockObj) {
				t1 = sdfMap.get(pattern);
				if(t1 == null){
					t1 = new ThreadLocal<SimpleDateFormat>(){
						@Override
						protected SimpleDateFormat initialValue() {
							return new SimpleDateFormat(pattern);
						}						
					};
					sdfMap.put(pattern, t1);
				}
			}
    	}
    	return t1.get();
    }
    
    /**
     * 获取当前日期字符串
     * 
     * @return 当前日期字符串，格式为： yyyy-MM-dd HH:mm:ss
     */
    public static String getCurrentDateTime() {
        return getCurrentDate() + " " + getCurrentTime();
    }

    /**
     * 获取当前日期字符串
     * 
     * @return 当前日期字符串,格式： yyyy-MM-dd
     */
    public static String getCurrentDate() {
        String fullDate = getCurrentYearMonth();
        int iCurrentDay = getCurrentDay();
        if (iCurrentDay < 10) {
            fullDate += "-0" + iCurrentDay;
        } else {
            fullDate += "-" + iCurrentDay;
        }
        return fullDate;
    }

    /**
     * 获取当前年月日期字符串
     * @return 当前年月日期字符串,格式： yyyy-MM
     */
    public static String getCurrentYearMonth() {
        return getSdf(YYYY_MM).format(new Date());
    }

    /**
     * 获取当前日期时、分、秒组成字符串
     * 
     * @return 当前日期时、分、秒组成字符串， 格式： HH:mm:ss
     */
    public static String getCurrentTime() {
        String time = getCurrentHourMinute();
        int iCurrentSecond = getCurrentSecond();
        if (iCurrentSecond < 10) {
            time += ":0" + iCurrentSecond;
        } else {
            time += ":" + iCurrentSecond;
        }
        return time;
    }

    /**
     * 获取当前日期时、分组成字符串
     * 
     * @return 当前日期时、分、秒组成字符串， 格式： HH:mm
     */
    public static String getCurrentHourMinute() {
        String time;
        int temp = getCurrentHour();
        if (temp < 10) {
            time = "0" + temp + ":";
        } else {
            time = temp + ":";
        }
        temp = getCurrentMinute();
        if (temp < 10) {
            time += "0" + temp;
        } else {
            time += temp;
        }
        return time;
    }

    /**
     * 获取当前日期-年份
     * 
     * @return 当前日期-年份
     */
    public static int getCurrentYear() {
        return Calendar.getInstance().get(Calendar.YEAR);
    }

    /**
     * 获取当前日期-月份
     * 
     * @return 当前日期-月份
     */
    public static int getCurrentMonth() {
        return Calendar.getInstance().get(Calendar.MONTH) + 1;
    }

    /**
     * 获取当前日期-在一年中第几周
     * @return  当前在一年中第几周
     */
    public static int getCurrentWeek(){
        return Calendar.getInstance().get(Calendar.WEEK_OF_YEAR);
    }
    
    /**
     * 获取当前日期-日数
     * 
     * @return 当前日期-日数
     */
    public static int getCurrentDay() {
        return Calendar.getInstance().get(Calendar.DAY_OF_MONTH);
    }

    /**
     * 获取当前日期-小时
     * 
     * @return 当前日期-小时
     */
    public static int getCurrentHour() {
        return Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
    }

    /**
     * 获取当前日期-分钟
     * 
     * @return 当前日期-分钟
     */
    public static int getCurrentMinute() {
        return Calendar.getInstance().get(Calendar.MINUTE);
    }

    /**
     * 获取当前日期-秒钟
     * 
     * @return 当前日期-秒钟
     */
    public static int getCurrentSecond() {
        return Calendar.getInstance().get(Calendar.SECOND);
    }

    /**
     * 得到一个日期对象
     * 
     * @return Date
     */
    public static Date getDate() {
        return new Date(System.currentTimeMillis());
    }

    /**
     * 得到一个SQL日期对象
     * 
     * @return Date
     */
    public static java.sql.Date getSQLDate() {
        return new java.sql.Date(System.currentTimeMillis());
    }

    /**
     * 得到一个SQL日期对象
     * 
     * @return java.sql.Timestamp
     */
    public static java.sql.Timestamp getSQLTimestamp() {
        return new java.sql.Timestamp(System.currentTimeMillis());
    }

    /**
     * 获取当前日期(一个SQL日期对象)
     * @return  当前日期(一个SQL日期对象)
     */
    public final static java.sql.Timestamp nowTimestamp() {
        return getSQLTimestamp();
    }
    
    /**
     * 判断闰年的方法
     * 
     * @param year  int 年份
     * @return boolean（true=闰年，false=非闰年）
     */
    public static boolean isLeapYear(int year) {
        if ((year % 400 == 0) || ((year % 4 == 0) && (year % 100 != 0))) {
            return true;
        }
        return false;
    }

    /**
     * 获取得到一年的天数的方法
     * 
     * @param year  int 年份
     * @return int （非闰年=365，闰年=366）
     */
    public static int getYearDays(int year) {
        int dayCount = 0;
        if (isLeapYear(year)) {
            dayCount = 366;
        }else{
            dayCount = 365;
        }
        return dayCount;
    }

    /**
     * 根据比较深度来比较两个日期，并且按比较深度返回d1-d2
     * 
     * @param d1    Date
     * @param d2    Date
     * @param scope int 比较级别
     * @return long
     */
    public static long compareDate(Date d1, Date d2, int scope) {
        switch (scope) {
        case ON_YEAR:
            return compareDateOnYear(d1, d2);
        case ON_MONTH:
            return compareDateOnMonth(d1, d2);
        case ON_DAY:
            return compareDateOnDay(d1, d2);
        case ON_HOUR:
            return compareDateOnHour(d1, d2);
        case ON_MINUTE:
            return compareDateOnMinute(d1, d2);
        case ON_SECOND:
            return compareDateOnSecond(d1, d2);
        case ON_SYSTEM:
            return compareDateOnSystem(d1, d2);
        default:
            return ERROR;
        }
    }

    /**
     * 比较两个日期的年份差距
     * 
     * @param d1    Date
     * @param d2    Date
     * @return int
     */
    public static int compareDateOnYear(Date d1, Date d2) {
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        c1.setTime(d1);
        c2.setTime(d2);
        int y1 = c1.get(Calendar.YEAR);
        int y2 = c2.get(Calendar.YEAR);
        return y1 - y2;
    }

    /**
     * 比较两个日期在月份上的差距
     * @param d1    Date
     * @param d2    Date
     * @return int
     */
    public static int compareDateOnMonth(Date d1, Date d2) {
        if (d1.getTime() == d2.getTime()) {
            return 0; // 日期相同返回0
        }
        int flag = -1;
        // 比较两个日期使日期较小的日期排在前面
        if (d1.getTime() > d2.getTime()) { // 日期一在日期二之后
            Date temp = d1;
            d1 = d2;
            d2 = temp;
            flag = 1;
        }
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        c1.setTime(d1);
        c2.setTime(d2);
        int y1 = c1.get(Calendar.YEAR);
        int y2 = c2.get(Calendar.YEAR);
        int mon1 = c1.get(Calendar.MONTH);
        int mon2 = c2.get(Calendar.MONTH);
        int mons = 0;
        for (int i = 1; i <= y2 - y1; i++) {
            mons += YEAR_MONTHS; // 一年有12个月
        }
        return (mons - mon1 + mon2) * flag;
    }

    /**
     * 比较两个日期并返回两个日期相差多少天(d1－d2)
     * @param d1    Date
     * @param d2    Date
     * @return int
     */
    public static int compareDateOnDay(Date d1, Date d2) {
        return (int)Math.ceil(Double.valueOf((getMorning(d1).getTime() - getMorning(d2).getTime()))/86400000);
    }

    /**
     * 比较两个日期并返回两个日期相差多少小时(d1－d2)
     * 
     * @param d1    Date
     * @param d2    Date
     * @return int
     */
    public static long compareDateOnHour(Date d1, Date d2) {
    	return (d1.getTime() - d2.getTime()) / (1000 * 60 * 60);
    }

    /**
     * 比较两个日期并返回两个日期相差多少分钟(d1－d2)
     * 
     * @param d1    Date
     * @param d2    Date
     * @return int
     */
    public static long compareDateOnMinute(Date d1, Date d2) {
        return (d1.getTime() - d2.getTime()) / (1000 * 60);
    }

    /**
     * 比较两个日期并返回两个日期相差多少秒(d1－d2)
     * 
     * @param d1    Date
     * @param d2    Date
     * @return int
     */
    public static long compareDateOnSecond(Date d1, Date d2) {
        return (d1.getTime() - d2.getTime()) / 1000;
    }

    /**
     * 比较两个时期返回两个日期的毫秒
     * @param d1
     * @param d2
     * @return
     */
    public static long compareDateOnSystem(Date d1, Date d2) {
        return d1.getTime() - d2.getTime();
    }

    /**
     * 格式化一个日期对象，默认的格式：yyyy-MM-dd HH:mm:ss
     * 
     * @param date  Date
     * @param pattern   String
     * @return String
     */
    public static String format(Date date, String pattern) {
        String fmtStr = "";
        if (date != null) {
            if(pattern == null || pattern.equals("")){
                pattern = "yyyy-MM-dd HH:mm:ss";
            }
            DateFormat df = new SimpleDateFormat(pattern);
            fmtStr = df.format(date);
        }
        return fmtStr;
    }

    /**
     * 将一个字符串格式化为一个java.util.Date对象
     * 
     * @param obj   Object
     * @return      Date
     */
    public static Date parse(Object obj) {
        try {
            String dateString;
            if (obj == null || (dateString = obj.toString().trim()).equals("")) {
                return null;
            }
            if (dateString.length() <= 10) {
                dateString += " 00:00:00";
            }
            return getSdf(YYYY_MM_DD_HH_MM_SS).parse(dateString);
        } catch (ParseException ex) {
            ex.printStackTrace();
            return null;
        }
    }
    
    /**
     * 将一个字符串格式化为一个Date对象
     * @param data 字符串日期
     * @param pattern 格式化样式
     * @return
     */
    public static Date parse(String data, String pattern) {
        try {
            SimpleDateFormat sf = new SimpleDateFormat(pattern);
            return sf.parse(data);
        } catch (ParseException ex) {
            ex.printStackTrace();
            return null;
        }
    }
    
    /**
     * 将一个字符串日期转换为Timestamp类型
     * @param strDate   符串日期
     * @return          Timestamp 类型日期
     */
    public static Date stringToDate(String strDate) {
        Date timestamp = null;
        try {
            if (strDate != null && !strDate.equals("")) {
                timestamp = new java.sql.Date(parse(strDate).getTime());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return timestamp;
    }

    /**
     * 格式化一个日期对象，格式：yyyy-MM-dd HH:mm:ss
     * 
     * @param date  要进行格式化的日期
     * @return 格式完后的日期字符串
     */
    public static String format(Date date) {
        return format(date, "yyyy-MM-dd HH:mm:ss");
    }
    
    public static String formatMMDD(Date date){
    	return getSdf(MM_DD).format(date);
    }
    
    /**
     * 格式化一个时间对象，格式：HH:mm:ss
     * 
     * @param date  要进行格式化的日期
     * @return 格式完后的日期字符串
     */
    public static String formatTime(Date date) {
        return format(date, "HH:mm:ss");
    }

    /**
     * 根据指定的pattern格式化类型为String的日期
     * 
     * @param dateStr   要进行格式化的日期串
     * @param pattern   格式化样式
     * @return          格式化完成后的日期串
     */
    public static String format(String dateStr, String pattern) {
        Date date = null;
        if (dateStr != null && !dateStr.equals("")) {
            try {
                SimpleDateFormat df = null;
                if (dateStr != null && dateStr.length() <= 10) {
                    df = new SimpleDateFormat("yyyy-MM-dd", java.util.Locale.getDefault());
                } else if(dateStr.split(":").length == 3){
                    df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", java.util.Locale.getDefault());
                } 
                if(df != null){
                    date = df.parse(dateStr);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return format(date, pattern);
    }
    
    /**
     * 格式化yyyy-MM-dd HH:mm:ss
     * @param date
     * @return
     */
    public final static String dateFormat(Date date){
    	return getSdf(YYYY_MM_DD_HH_MM_SS).format(date);
    }

    @SuppressWarnings({"unchecked"})
    public final static List separateDateStr(String startDate, String endDate, long step) {
        List rtn = separateDate(startDate, endDate, step);
        int size = rtn.size();
        for (int i = 0; i < size; i++) {
            rtn.set(i, getSdf(YYYY_MM_DD_HH_MM_SS).format(rtn.get(i)));
        }
        return rtn;
    }

    public final static List<Date> separateDate(String startDate, String endDate, long step) {
        Date d1 = parse(startDate);
        Date d2 = parse(endDate);
        long start = d1.getTime();
        long end = d2.getTime();
        if (start > end) {
            throw new IllegalArgumentException("开始日期（" + startDate + "）晚于结束日期（" + endDate + "）");
        }
        List<Date> rtn = new ArrayList<Date>();
        long tmp = start;
        while (end > tmp) {
            rtn.add(new Date(tmp));
            tmp += step;
        }
        rtn.add(new Date(tmp));
        return rtn;
    }
    
    public final static List<String> getSeparateStr(String startDate, String endDate) {
    	List<Date> list = separateDate(startDate, endDate, DAY_MSELS);
    	List<String> arr = new ArrayList<String>();
    	for(Date d : list){
    		arr.add(format(d, YYYY_MM_DD));
    	}
    	Collections.sort(arr, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return -o1.compareTo(o2);
			}
		});
    	
        return arr;
    }

    /**
     * 比较日期字符串大小 如果d1>d2返回1 如果d1=d2返回0 如果d1<d2返回-1
     * 
     * @param d1    日期一
     * @param d2    日期二
     */
    public static int compareDateStr(String d1, String d2) {
        Date date1 = parse(d1);
        Date date2 = parse(d2);
        return new Long(date1.getTime()).compareTo(new Long(date2.getTime()));
    }
    
    /**
     * 比较日期大小 如果d1>d2返回1 如果d1=d2返回0 如果d1<d2返回-1
     * 
     * @param d1    日期一
     * @param d2    日期二
     */
    public static int compareDate(Date date1, Date date2) {
        return new Long(date1.getTime()).compareTo(new Long(date2.getTime()));
    }


    /**
     * 日期加减
     * 
     * @param format        y:年运算 m:月运算 d：日运算
     * @param number        加减天数
     * @param strYYYYMMDD   被运算日期
     * @return              运算后结果日期
     */
    public static String addDate(String format, int number, String strYYYYMMDD) {
        Calendar cal = null;
        String strDate = "";
        Date date = null;
        getSdf(YYYY_MM_DD).setLenient(false);
        try {
            cal = Calendar.getInstance();
            cal.clear();
            date = getSdf(YYYY_MM_DD).parse(strYYYYMMDD);
            cal.setTime(date);
            if (format.toLowerCase().equals("y") == true) {
                cal.add(Calendar.YEAR, number);
            } else {
                if (format.toLowerCase().equals("m") == true) {
                    cal.add(Calendar.MONTH, number);
                } else {
                    cal.add(Calendar.DATE, number);
                }
            }
            date = cal.getTime();
            strDate = getSdf(YYYY_MM_DD).format(date);
        } catch (Exception e) {
            strDate = "";
        }
        return strDate;
    }

    /**
     * 在日期上加一定的毫秒得到一个新日期
     * @param date 被运算的日期
     * @param milliseconds 加减的毫秒数
     * @return
     */
    public static Date addMillisecond(Date date, long milliseconds) {
        long baseMilliseconds = date.getTime();
        Date newDate = new Date();
        newDate.setTime(baseMilliseconds + milliseconds);
        return newDate;
    }

    /**
     * 在用户查询的日期后面加上时分秒,以便能更精确查询到结果
     * @param strDate  要构造的日期,如:2007-10-10
     * @param HHmmss 在传过来的日期后加上时分秒,如:00:00:00
     * @return  2007-10-10 00:00:00
     */
    public static Date constructDate(String strDate, String HHmmss) {
        try {
            if (strDate == null || strDate.length() < 1) return null;
            try {
                String dateTemp = strDate + " " + HHmmss;
                Date date = getSdf(YYYY_MM_DD_HH_MM_SS).parse(dateTemp);
                return date;
            } catch (ParseException ex) {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * 获取给定日期是周几
     * @param date
     * @return 1，2，3，4，5，6，7
     */
    public static int getDayOfWeek(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        int week = c.get(Calendar.DAY_OF_WEEK) - 1;
        week = week < 1 ? 7 : week;
        return week;
    }
    
    /**
     * 获取给定日期是一月的第几天
     * @param date
     * @return
     */
    public static int getDayOfMonth(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.get(Calendar.DAY_OF_MONTH);
    }

    //  获得周一的日期   
    public static String getMonday(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        return new SimpleDateFormat("yyyy-MM-dd").format(c.getTime());
    }

    // 获得周五的日期
    public static String getFriday(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.set(Calendar.DAY_OF_WEEK, Calendar.FRIDAY);
        return new SimpleDateFormat("yyyy-MM-dd").format(c.getTime());
    }

    //给定日期前几天或者后几天的日期   
    public static String afterNDay(Date nowDate, int n) {
        Calendar c = Calendar.getInstance();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        c.setTime(nowDate);
        c.add(Calendar.DATE, n);
        Date d2 = c.getTime();
        String s = df.format(d2);
        return s;
    }
    
   /** 给定日期前几天或者后几天的日期    */
    public static Date afterNowNDay(Date nowDate, int n) {
        Calendar c = Calendar.getInstance();
        c.setTime(nowDate);
        c.add(Calendar.DATE, n);
        return new Date(c.getTime().getTime());
    }
    
    //获取当前日期前几天或者后几天的日期   
    public static Date afterNowNDay(int n) {
        Calendar c = Calendar.getInstance();
        c.setTime(nowTimestamp());
        c.add(Calendar.DATE, n);
        return new Date(c.getTime().getTime());
    }
    
    // 判断两个日期是否在同一周
    public boolean isSameWeekDates(Date date1, Date date2) {
        Calendar cal1 = Calendar.getInstance();
        Calendar cal2 = Calendar.getInstance();
        cal1.setTime(date1);
        cal2.setTime(date2);
        int subYear = cal1.get(Calendar.YEAR) - cal2.get(Calendar.YEAR);
        if (0 == subYear) {
            if (cal1.get(Calendar.WEEK_OF_YEAR) == cal2.get(Calendar.WEEK_OF_YEAR)) return true;
        } else if (1 == subYear && 11 == cal2.get(Calendar.MONTH)) {
            // 如果12月的最后一周横跨来年第一周的话则最后一周即算做来年的第一周
            if (cal1.get(Calendar.WEEK_OF_YEAR) == cal2.get(Calendar.WEEK_OF_YEAR)) return true;
        } else if (-1 == subYear && 11 == cal1.get(Calendar.MONTH)) {
            if (cal1.get(Calendar.WEEK_OF_YEAR) == cal2.get(Calendar.WEEK_OF_YEAR)) return true;
        }
        return false;
    }

    // 产生周序列
    public static String getSeqWeek(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        String week = Integer.toString(c.get(Calendar.WEEK_OF_YEAR));
        if (week.length() == 1) week = "0" + week;
        String year = Integer.toString(c.get(Calendar.YEAR));
        return year + "-" + week;
    }

    /**
     * 判断两个日期是否为同一天(2014年5月1日和2015年5月1日会认为是同一天)
     * @param date
     * @param otherDate
     * @return
     */
    public static boolean isSameDay(Date date1, Date date2) {
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(date1);
        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(date2);
        return cal1.get(Calendar.DAY_OF_YEAR)==cal2.get(Calendar.DAY_OF_YEAR);
          
    }
    /**
     * 判断两个日期是否为同一个月(2014年5月和2015年5月会认为是同一个月)
     * @param date
     * @param otherDate
     * @return
     */
    public static boolean isSameMonth(Date date1, Date date2) {
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(date1);
        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(date2);
        return cal1.get(Calendar.MONTH)==cal2.get(Calendar.MONTH);
          
    }
    
    /**
    * 取得当月天数
    */
    public static int getCurrentMonthLastDay(){
	    Calendar a = Calendar.getInstance();
	    a.set(Calendar.DATE, 1);//把日期设置为当月第一天
	    a.roll(Calendar.DATE, -1);//日期回滚一天，也就是最后一天
	    int maxDate = a.get(Calendar.DATE);
	    return maxDate;
    }
    
    /**
    * 取得当月第一天
    */
    public static Date getCurrentMonthFirstDay(){
        Calendar a = Calendar.getInstance();
        a.set(Calendar.DATE, 1);//把日期设置为当月第一天
        return a.getTime();
    }
    
    /**
	 * 格式HH:mm:ss
	 * 
	 * @return
	 */
    public static Date parseDate(String HHmmss) {
		Calendar cal = Calendar.getInstance();
		String[] HH_mm_ss = HHmmss.split(":");
		cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(HH_mm_ss[0].trim()));
		cal.set(Calendar.MINUTE, Integer.parseInt(HH_mm_ss[1].trim()));
		cal.set(Calendar.SECOND, Integer.parseInt(HH_mm_ss[2].trim()));
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}

    /**
     * 判断时间是否是在指定时间内
     */
	public static boolean isBetweenTime(Date date, Date[] scope) {
		return (date.compareTo(scope[0]) >= 0 && date.compareTo(scope[1]) < 0);
	}
	
	/**
     * 获取指定时间当天的凌晨
     * 
     * @param date
     *            给定时间当天的凌晨
     * @return
     */
    public static Date getMorning(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTime();
    }
    
    /**
     * 格式HH:mm
     * @return
     */
    public static Date parseDateByHHMM(String HHmm) {
        Calendar cal = Calendar.getInstance();
        String[] HH_mm = HHmm.split(":");
        cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(HH_mm[0].trim()));
        cal.set(Calendar.MINUTE, Integer.parseInt(HH_mm[1].trim()));
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }
    
    
    public static void main(String[] s) {
        //  String d1 = "2003-12-31 23:59:59";
        //  String d2 = "2003-12-31 23:59:58";
        //  Date t1 = parse(d1, YYYY_MM_DD_HH_MM_SS);
        //  Date t2 = parse(d2, YYYY_MM_DD_HH_MM_SS);
        //  System.out.println(compareDateOnDay(t2,t1));
        //    //System.out.println("" + parse(d1).getTime());
        //    String source = "00:10:00";
        //    long step = convertToMSEL(source, PATTERN_DDHHMM);
        //    java.util.List l = separateDate(d1, d2, step);
        //    for (int i = 0; i < l.size(); i++) {
        //      //System.out.println("【" + i + "】：＝" + l.get(i));
        //    } // End for-i
        //    String date = format(new Date(), "MM/dd/yyyy HH:mm:ss"); //"yyyy-MM-dd HH:mm:ss"
        //    System.out.println("date = " + date);
        //      System.out.println("sss:"+format("2006-08-26","yyyy年MM月dd日"));
        //System.out.println(DateUtil.nowTimestamp());
//    	Calendar calendar = Calendar.getInstance();
//		calendar.add(Calendar.DAY_OF_YEAR, -1);
//    	System.out.println(format(calendar.getTime(), YYYY_MM_DD_));
    	
//    	List<String> list = getSeparateStr("2017-03-01", "2017-03-10");
//    	for(String ss : list){
//    		System.out.println(ss);
//    	}
    	
    	System.out.println(format(new Date(), HH));
    	
    }
}
