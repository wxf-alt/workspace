package waterMark02.custom_watermark;

import bean.Sensor;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @Auther: wxf
 * @Date: 2024/6/11 09:35:00
 * @Description: CustomBoundedOutOfOrdernessGenerator   自定义周期性 Watermark 生成器
 * @Version 1.0.0
 */
public class CustomBoundedOutOfOrdernessGenerator implements WatermarkGenerator<Sensor> {

    private final long maxOutOfOrderness = 3500; // 3.5 秒
    private long currentMaxTimestamp = Long.MIN_VALUE + maxOutOfOrderness + 1;

    // 该 watermark 生成器可以覆盖的场景是：数据源在一定程度上乱序。 即某个最新到达的时间戳为 t 的元素将在最早到达的时间戳为 t 的元素之后最多 n 毫秒到达。
    @Override
    public void onEvent(Sensor event, long eventTimestamp, WatermarkOutput output) {

        System.out.println("event - getTimeStamp：" + event.getTimeStamp());
        System.out.println("eventTimestamp：" + eventTimestamp);

        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);

        System.out.println("currentMaxTimestamp：" + currentMaxTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
    }

}