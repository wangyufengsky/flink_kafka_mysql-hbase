package kafkatoflink;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
/**
 * @Description    MessageWaterEmitter 根据Kafka消息确定Flink的水位
 */
public class MessageWaterEmitter implements AssignerWithPunctuatedWatermarks<String> {

    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
        if (lastElement != null && lastElement.contains(",")) {
            String[] parts = lastElement.split(",");
            return new Watermark(Long.parseLong(parts[0]));
        }
        return null;
    }

    public long extractTimestamp(String element, long previousElementTimestamp) {
        if (element != null && element.contains(",")) {
            String[] parts = element.split(",");
            return Long.parseLong(parts[0]);
        }
        return 0L;
    }
}
