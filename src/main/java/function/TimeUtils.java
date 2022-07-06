package function;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author wfs
 */
public class TimeUtils {

    public static final int TEN = 10;
    public static final int THIRTEEN = 13;
    public static final DateTimeFormatter DATETIMEFORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 根据string的位数分类转为datetime 对于2001-09-09 09:46:39以后的时间来说，秒级utc都是10位
     *
     * @param epoch utc时间，秒级或者毫秒级，10位或者13位
     * @param defaultValue 出错时候默认返回值
     * @return 日期或者null
     */
    public static LocalDateTime utc2DateTime(String epoch, LocalDateTime defaultValue) {
        try {
            if (epoch.length() == TEN) {
                return LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(Long.parseLong(epoch)), ZoneId.of("Asia/Shanghai"));
            } else if (epoch.length() == THIRTEEN) {
                return LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(Long.parseLong(epoch)), ZoneId.of("Asia/Shanghai"));
            } else {
                return null;
            }
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * 已知该epoch是秒级还是毫秒级
     *
     * @param epoch utc时间
     * @param isSecond 秒级填true，毫秒填false
     * @param defaultValue 出错时候默认返回值
     * @return 日期或者null
     */
    public static LocalDateTime utc2DateTime(
            String epoch, boolean isSecond, LocalDateTime defaultValue) {
        try {
            if (isSecond) {
                return LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(Long.parseLong(epoch)), ZoneId.of("Asia/Shanghai"));
            } else {
                return LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(Long.parseLong(epoch)), ZoneId.of("Asia/Shanghai"));
            }
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * 已知该epoch是秒级还是毫秒级
     *
     * @param epoch utc时间
     * @param isSecond 秒级填true，毫秒填false
     * @param defaultValue 出错时候默认返回值
     * @return 日期或者null
     */
    public static LocalDateTime utc2DateTime(
            long epoch, boolean isSecond, LocalDateTime defaultValue) {
        try {
            if (isSecond) {
                return LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(epoch), ZoneId.of("Asia/Shanghai"));
            } else {
                return LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(epoch), ZoneId.of("Asia/Shanghai"));
            }
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * 将long值转换为yyyy-MM-dd HH:mm:ss
     *
     * @param epoch utc时间
     * @param isSecond 秒级填true，毫秒填false
     * @param defaultValue 出错时候默认返回值
     * @return yyyy-MM-dd HH:mm:ss格式的string
     */
    public static String utc2parsedString(long epoch, boolean isSecond, String defaultValue) {
        try {
            if (isSecond) {
                return DATETIMEFORMATTER.format(
                        LocalDateTime.ofInstant(
                                Instant.ofEpochSecond(epoch), ZoneId.of("Asia/Shanghai")));
            } else {
                return DATETIMEFORMATTER.format(
                        LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(epoch), ZoneId.of("Asia/Shanghai")));
            }
        } catch (Exception e) {
            return defaultValue;
        }
    }
}
