package io.tebbe;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by ct.
 */
public class Utils {
    public static final String FORMAT = "%02d";

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
            return Integer.parseInt(str);
        } catch(Exception e) {

        } finally {
            return 0;
        }
    }

    public static boolean isIntegerValue(Text query) {
        String str = query.toString();
        try {
            Integer.parseInt(str);
            return true;
        } catch(Exception e) {}
        finally {
            return false;
        }
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
