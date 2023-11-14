import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Auther: wxf
 * @Date: 2022/12/5 18:13:01
 * @Description: MyInterceptor
 * @Version 1.0.0
 */
public class MyInterceptor implements Interceptor {

    private List<Event> results = new ArrayList<>();
    private String startFlag = "\"en\":\"start\"";

    @Override
    public void initialize() {

    }

    @Override
    // 拦截逻辑
    public Event intercept(Event event) {
        // 在 header中添加key
        Map<String, String> headers = event.getHeaders();
        byte[] body = event.getBody();
        String str = new String(body, Charset.forName("UTF-8"));
        boolean flag = true;
        if (str.contains(startFlag)) {
            headers.put("topic", "topic_start");
            flag = ETLUtil.validStartLog(str);
        }else{
            headers.put("topic", "topic_event");
            flag = ETLUtil.validEventLog(str);
        }

        // 判断验证结果
        if (!flag) {
            return null;
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        results.clear();

        for (Event event : list) {
            Event result = intercept(event);
            if (null != result) {
                results.add(result);
            }
        }
        return results;
    }

    @Override
    public void close() {

    }


    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        // 从 flume 配置文件中读取配置
        public void configure(Context context) {

        }
    }

}