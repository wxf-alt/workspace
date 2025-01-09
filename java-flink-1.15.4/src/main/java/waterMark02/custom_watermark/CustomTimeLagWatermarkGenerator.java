package waterMark02.custom_watermark;

import bean.Sensor;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @Auther: wxf
 * @Date: 2024/6/11 09:36:11
 * @Description: CustomTimeLagWatermarkGenerator  自定义周期性 Watermark 生成器
 * @Version 1.0.0
 */
public class CustomTimeLagWatermarkGenerator implements WatermarkGenerator<Sensor> {

    private final long maxTimeLag = 5000; // 5 秒

    @Override
    public void onEvent(Sensor event, long eventTimestamp, WatermarkOutput output) {
        // 处理时间场景下不需要实现
    }

    // 该生成器生成的 watermark 滞后于处理时间固定量。它假定元素会在有限延迟后到达
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
    }
}

