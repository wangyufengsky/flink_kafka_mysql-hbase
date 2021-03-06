package Hbase;

import entity.Detail;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HbaseSink4DetailTestTuple extends RichSinkFunction<List<Tuple6<String,String,String,String,String,String>>> implements Serializable {


    private static final long serialVersionUID = 1L;
    private org.apache.hadoop.conf.Configuration configuration=null;
    private Connection connection ;
    private Table detailTable ;
    private Admin admin;
    private String framliyClf = "mainInfoCLF";
    private IntCounter nums=new IntCounter();


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node1,node2,node3,node4,node5,node6,node8,node9,node10,node11,node12,node13,node14,node15,node16,node17,node18,node19");
        configuration.set("fs.defaultFS", "hdfs://node1:9000");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily","3200");
        connection = ConnectionFactory.createConnection(configuration);
        detailTable = connection.getTable(TableName.valueOf("detail"));
        admin=connection.getAdmin();
        getRuntimeContext().addAccumulator("DetailNums",nums);
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }



    @Override
    public void invoke(List<Tuple6<String,String,String,String,String,String>> details,Context context) throws Exception {
        //Tuple6<String,String,String,String,String,String> customer_id event_date event_gmt busi_code busi_amount busi_amount
        long startTime=System.currentTimeMillis();
        List<Put> list = new ArrayList<>();
        int i=0;
/*       alllist=alllist.stream().collect(Collectors.toMap(Detail::getOnlyone, a->a,(o1, o2)->{
            o1.setMoney(o1.getMoney().add(o2.getMoney()));
            return o1;
        })).values().stream().collect(Collectors.toList());*/
        for(Tuple6<String,String,String,String,String,String> detail:details){
            Put put1 = new Put(Bytes.toBytes(detail.f0+detail.f1));
            put1.addColumn(Bytes.toBytes(framliyClf), Bytes.toBytes(detail.f3+System.currentTimeMillis()+i), Bytes.toBytes(detail.f2+","+detail.f4+","+detail.f5));
            list.add(put1);
            i++;
        }
        detailTable.put(list);
        long endTime=System.currentTimeMillis();
        System.out.println("已插入"+nums+"条明细数据,用时:"+(endTime-startTime));
        nums.add(details.size());
    }


}
