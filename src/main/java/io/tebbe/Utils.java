package io.tebbe;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by ct.
 */
public class Utils {
    public static final String FORMAT = "%02d";

    public static void main(String[] args) {
        System.out.println(isIntegerValue("Lehigh airport, ".split(",")[1]));
        System.out.println(isIntegerValue("1987,1258".split(",")[1]));
    }

    public static IntWritable getSumDelay(String[] fields) {
        return new IntWritable(
            Utils.getInt(fields[Index.ARRDELAY]) +
            Utils.getInt(fields[Index.DEPDELAY]) +
            Utils.getInt(fields[Index.CARRIERDELAY]) +
            Utils.getInt(fields[Index.WEATHERDELAY]) +
            Utils.getInt(fields[Index.NASDELAY]) +
            Utils.getInt(fields[Index.SECURITYDELAY]) +
            Utils.getInt(fields[Index.LATEAIRCRAFTDELAY]));
    }

    public static int getInt(String str) {
        try {
            System.out.println(str);
            int tmp = Integer.parseInt(str);
            if(tmp < 0) tmp=0;
            return tmp;
        } catch(NumberFormatException e) {
            //e.printStackTrace();
        }
        return 0;
    }

    public static boolean isIntegerValue(String query) {
        try {
            Integer.parseInt(query.trim());
            return true;
        } catch(Exception e) {}
        return false;
    }

    public static boolean isIntegerValue(Text query) {
        return isIntegerValue(query.toString());
    }

    public static String getTimeOfDay(String hhmm) {
        int hour = Integer.parseInt(hhmm.substring(0, hhmm.length()-2));
        return String.format(Utils.FORMAT, hour);
    }

    public static String getDayOfWeek(String d) {
        return String.format(Utils.FORMAT, Integer.parseInt(d));
    }

    public static String getMonth(String mm) {
        return String.format(Utils.FORMAT, Integer.parseInt(mm));
    }
}
