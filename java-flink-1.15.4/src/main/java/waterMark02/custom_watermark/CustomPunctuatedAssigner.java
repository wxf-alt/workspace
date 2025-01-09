package waterMark02.custom_watermark;

import bean.Sensor;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @Auther: wxf
 * @Date: 2024/6/11 09:34:05
 * @Description: CustomPunctuatedAssigner  自定义标记性 Watermark 生成器
 * @Version 1.0.0
 */
public class CustomPunctuatedAssigner implements WatermarkGenerator<Sensor> {

    private long currentMaxTimestamp;
    private long nextWaterMark;

    @Override
    public void onEvent(Sensor event, long eventTimestamp, WatermarkOutput output) {
        if (null != event.getTimeStamp()) {
            currentMaxTimestamp = Math.max(Long.parseLong(event.getTimeStamp()), currentMaxTimestamp);

            System.out.println("event - getTimeStamp：" + event.getTimeStamp());
            System.out.println("eventTimestamp：" + eventTimestamp);
            System.out.println("currentMaxTimestamp：" + currentMaxTimestamp);

            output.emitWatermark(new Watermark(currentMaxTimestamp));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // onEvent 中已经实现
    }
}