package function;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author wfs
 */
public class Utils {
    private static final long EIGHT_HOUR_EPOCH = 28800000L;
    private static final long ONE_HOUR_SECOND = 3600;

    public static long isoTimeParse(String time, int hoursDiff) {
        Instant parse = Instant.parse(time);
        long epochSecond = parse.getEpochSecond();
        return epochSecond + hoursDiff * ONE_HOUR_SECOND;
    }

    public static long epochReduceEightHour(String epoch) {
        return Long.parseLong(epoch) - EIGHT_HOUR_EPOCH;
    }

    public static List<String> schemaToList(String schema) {
        try {
            List<String> schemaList = Arrays.asList(schema.split(","));
            if (schemaList.size() == 0) {
                throw new RuntimeException("schema split by ',',and size is 0;");
            } else {
                return schemaList;
            }
        } catch (Exception e) {
            throw new RuntimeException("schema split by ',',and size is 0;");
        }
    }

    public static Set<Integer> schemaNoToSet(String noString) {
        if ("".equals(noString)) {
            return null;
        }
        String[] split = noString.split(",");
        HashSet<Integer> set = new HashSet<>(split.length);
        for (String s : split) {
            int i = Integer.parseInt(s);
            set.add(i);
        }
        return set;
    }

}
