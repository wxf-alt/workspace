import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

/**
 * @Auther: wxf
 * @Date: 2022/12/5 18:27:39
 * @Description: ETLUtil
 * @Version 1.0.0
 */
public class ETLUtil {
    // 判断启动日志是否符合格式要求
    public static boolean validStartLog(String source) {
        // 判断body部分是否有数据
        if (StringUtils.isBlank(source)) {
            return false;
        }
        // 验证Json字符串的完整性,是否以{}开头结尾
        String trimStr = source.trim();
        if (trimStr.startsWith("{") && trimStr.endsWith("}")) {
            return true;
        }

        return false;
    }

    // 判断事件日志是否符合格式要求
    public static boolean validEventLog(String source) {
        // 判断body部分是否有数据
        if (StringUtils.isBlank(source)) {
            return false;
        }
        String trimStr = source.trim();
        // 判断时间戳
        String[] split = trimStr.split("\\|");
        if (split.length == 2) {
            return false;
        }
        if (split[0].length() != 13 && !NumberUtils.isDigits(split[0])) {
            return false;
        }

        // 判断字符串
        if (split[1].startsWith("{") && split[1].endsWith("}")) {
            return true;
        }
        return false;
    }


}