package cn.edu.neu.tools;

import java.util.Calendar;

/**
 * @author liyu
 */
public class GetDay {
    public static int getDayNum() {
        Calendar c = Calendar.getInstance();
        return (int) (c.getTimeInMillis() / 1000 / 60 / 60 / 24);
    }
}
