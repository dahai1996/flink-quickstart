package function;

/**
 * @author wfs
 */
public class StringUtils {
    public static final String NULL_STRING = "null";
    public static final String EMPTY_STRING = "";

    public boolean isNull(String s) {
        return s == null || NULL_STRING.equals(s) || EMPTY_STRING.equals(s);
    }
}
