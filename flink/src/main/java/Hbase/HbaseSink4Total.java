package Hbase;

import Util.Tools;
import entity.Detail;
import entity.Total;
import entity.hbaseRow;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.StringUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HbaseSink4Total extends RichSinkFunction<List<Detail>> implements Serializable {
    private static final long serialVersionUID = 1L;
    private org.apache.hadoop.conf.Configuration configuration=null;
    private Connection connection ;
    private Table totalTable ;
    private Admin admin;
    private String framliyClf = "mainInfoCLF";
    private IntCounter nums=new IntCounter();
    List<Detail> alllist = new ArrayList<>();


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.setProperty("hadoop.home.dir","D:\\hadoop-common");
        configuration = HBaseConfiguration.create();
        /*configuration.set("hbase.zookeeper.quorum", "10.137.8.22,10.137.8.25,10.137.8.26,10.137.8.34,10.137.8.35,10.137.8.36,10.137.8.38,10.137.8.39,10.137.8.40," +
                "10.141.13.13,10.141.13.14,10.141.13.15,10.141.13.16,10.141.13.17,10.141.13.18,10.141.13.19,10.141.13.20,10.141.13.21");*/
        configuration.set("hbase.zookeeper.quorum", "node1,node2,node3,node4,node5,node6,node8,node9,node10,node11,node12,node13,node14,node15,node16,node17,node18,node19");
        configuration.set("fs.defaultFS", "hdfs://node1:9000");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily","3200");
        connection = ConnectionFactory.createConnection(configuration);
        totalTable = connection.getTable(TableName.valueOf("total"));
        admin=connection.getAdmin();
        getRuntimeContext().addAccumulator("TotalNums",nums);
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }



    @Override
    public void invoke(List<Detail> details,Context context) throws Exception {
        long startTime=System.currentTimeMillis();
        List<Put> listTotal=new ArrayList<>();
        List<Get> getList=new ArrayList<>();
        for(Detail detail:details){
            alllist.add(detail);
        }
        alllist=alllist.stream().collect(Collectors.toMap(Detail::getOnlyone,a->a,(o1,o2)->{
            o1.setMoney(o1.getMoney().add(o2.getMoney()));
            return o1;
        })).values().stream().collect(Collectors.toList());
        if(alllist.size()>=10000){
            for(Detail detail:alllist){
                Get get = new Get(Bytes.toBytes(detail.getRowKey()));
                get.addColumn(Bytes.toBytes(framliyClf),Bytes.toBytes(detail.getType()));
                getList.add(get);
            }
            long startTime1=System.currentTimeMillis();
            Result[] results=totalTable.get(getList);
            long endTime1=System.currentTimeMillis();
            List<hbaseRow> rowList=new ArrayList<>();
/*            if(results.length>0){
                rowList=Arrays.stream(results).map(result -> new hbaseRow(Bytes.toString(CellUtil.cloneValue(result.rawCells()[0])),
                        Bytes.toString(CellUtil.cloneFamily(result.rawCells()[0])),
                        Bytes.toString(CellUtil.cloneQualifier(result.rawCells()[0])),
                        new BigDecimal(Bytes.toString(CellUtil.cloneValue(result.rawCells()[0])))))
                        .collect(Collectors.toList())
                        .stream()
                        .filter(Tools.distinctByKey(o->o.getOnlyOne()))
                        .collect(Collectors.toList());
            }*/
            for(Result result:results){
                for(Cell kv:result.rawCells()){
                    hbaseRow cell=new hbaseRow(Bytes.toString(CellUtil.cloneValue(kv)),
                            Bytes.toString(CellUtil.cloneFamily(kv)),
                            Bytes.toString(CellUtil.cloneQualifier(kv)),
                            new BigDecimal(Bytes.toString(CellUtil.cloneValue(kv))));
                    rowList.add(cell);
                }
            }
            if(rowList.size()>1){
                rowList.stream().filter(Tools.distinctByKey(o->o.getOnlyOne())).collect(Collectors.toList());
            }
            List<hbaseRow> newRow=alllist.stream().map(rr->new hbaseRow(rr.getRowKey(),framliyClf,rr.getType(),rr.getMoney())).collect(Collectors.toList());
            newRow= Stream.concat(rowList.stream(),newRow.stream())
                    .collect(Collectors.toMap(hbaseRow::getOnlyOne,a->a,(o1,o2)->{
                        o1.setValue(o1.getValue().add(o2.getValue()));
                        return o1;
                    })).values().stream().collect(Collectors.toList());
            for(hbaseRow hbase:newRow){
                Put put = new Put(Bytes.toBytes(hbase.getRowKey()));
                put.addColumn(Bytes.toBytes(framliyClf), Bytes.toBytes(hbase.getColunm()), Bytes.toBytes(hbase.getValue().toString()));
                listTotal.add(put);
            }
            long startTime2=System.currentTimeMillis();
            totalTable.put(listTotal);
            long endTime=System.currentTimeMillis();
            System.out.println("已插入"+nums+"条全量数据,插入用时:"+(endTime-startTime2)+"查询耗时："+(endTime1-startTime1)+"总耗时:"+(endTime-startTime));
            alllist.clear();
        }
        nums.add(details.size());
    }




    public  String getByRowKey(String rowKey,String framliy,String column) throws Exception {
        Get g = new Get(rowKey.getBytes());
        g.addColumn(Bytes.toBytes(framliy),Bytes.toBytes(column));
        Result rs = totalTable.get(g);
        return buildCells(rs.rawCells());
    }


    private  String buildCells(Cell[] cells) {
        String all="";
        if (cells == null || cells.length == 0) {
            return null;
        }
        for(Cell cell : cells){
            String row = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            all=value;
        }
        return all;
    }


}
