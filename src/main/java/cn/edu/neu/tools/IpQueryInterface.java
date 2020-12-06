package cn.edu.neu.tools;

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
        return CityContainer.CITY_LIST[sum % CityContainer.CITY_LIST.length];
    }
}
