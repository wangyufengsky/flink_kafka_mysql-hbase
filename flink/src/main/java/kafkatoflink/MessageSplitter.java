package kafkatoflink;
import entity.Detail;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

/**
 * @Description    MessageSplitter 将获取到的每条Kafka消息根据“，”分割取出其中的主机名和内存数信息
 */
public class MessageSplitter implements FlatMapFunction<String, Tuple5<String,String,String,Double, String>> {

/*    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
        if (value != null && value.contains(",")) {
            String[] parts = value.split(",");
            out.collect(new Tuple2<>(parts[1], Long.parseLong(parts[2])));
        }
    }*/

    /*@Override
    public void flatMap(String value, Collector<Detail> out) throws Exception {
        if (value != null && value.contains(",")) {
            String[] parts = value.split(",");
            Detail detail=new Detail();
            detail.setType(parts[1]);
            detail.setMoney(new BigDecimal(parts[5]));
            detail.setCertNo(parts[7]);
            detail.setDate(parts[12]);
            out.collect(detail);
        }
    }*/


    @Override
    public void flatMap(String value, Collector<Tuple5<String, String, String, Double, String>> out) throws Exception {
        if (value != null && value.contains(",")) {
            String[] parts = value.split(",");
            //客户号、日期、类型、金额、总消息
            //out.collect(new Tuple5<>(parts[7],parts[12],parts[1],new BigDecimal(parts[5]),value));
            out.collect(new Tuple5<>(parts[7],parts[12],parts[1],Double.valueOf(parts[5]),value));
        }
    }
}
