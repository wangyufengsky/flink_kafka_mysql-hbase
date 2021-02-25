package RelTest.Sink;


import RelTest.InsertBean.DetailInsertBean;
import entity.DetailBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.*;

public class DetailSink extends RichSinkFunction<List<DetailInsertBean>> implements Serializable {

    private static String framliyClf = "mainInfoCLF";
    private static Table detailTable =null;
    private static org.apache.hadoop.hbase.client.Connection connection=null;
    private static int nums;

    public static void init(){
        try{
            org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "node1,node2,node3,node4,node5,node6,node8,node9,node10,node11,node12,node13,node14,node15,node16,node17,node18,node19");
            configuration.set("fs.defaultFS", "hdfs://node1:9000");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily","3200");
            connection= ConnectionFactory.createConnection(configuration);
            detailTable = connection.getTable(TableName.valueOf("detail"));
            Admin admin=connection.getAdmin();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    
    

    @Override
    public void open(Configuration parameters) {
        init();
    }



    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("已关闭！");
    }

    public static void put(List<DetailInsertBean> details, Table table){
        try {
            List<Put> list = new ArrayList<>();
            for(DetailInsertBean detail:details){
                Put put = new Put(Bytes.toBytes(detail.getCustomerId()+detail.getEventDate()));
                put.addColumn(Bytes.toBytes(framliyClf), Bytes.toBytes(detail.getBusiCode()+detail.getBusiTime()),
                        Bytes.toBytes(detail.getBusiAmount()+","+detail.getClearAmount()+","+detail.getBusiRemark()+","+detail.getEventId()));
                list.add(put);
            }
            table.put(list);
            nums+=details.size();
            list.clear();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(List<DetailInsertBean> value, Context context) throws Exception {
        Thread.sleep(1000);
        put(value,detailTable);
        System.out.println(nums);
    }



}
