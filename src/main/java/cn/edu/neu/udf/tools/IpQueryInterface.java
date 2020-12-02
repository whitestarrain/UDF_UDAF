package cn.edu.neu.udf.tools;

/**
 * @author liyu
 */
public class IpQueryInterface {
    public static String query(String str) {
        String[] split = str.split("\\.");
        int sum = 0;
        for (String s : split) {
            sum += Integer.parseInt(s);
        }
        return City.CITY_LIST[sum % City.CITY_LIST.length];
    }
}
