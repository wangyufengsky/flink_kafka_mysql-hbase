package Hbase;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import java.util.ArrayList;
import java.util.List;

public class test {
    private static String framliyClf = "mainInfoCLF";
    private static org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
    private static Connection connection=null;

    public static void main(String[] args) throws Exception{
        configuration.set("hbase.zookeeper.quorum", "node1,node2,node3,node4,node5,node6,node8,node9,node10,node11,node12,node13,node14,node15,node16,node17,node18,node19");
        configuration.set("fs.defaultFS", "hdfs://node1:9000");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily","3200");
        connection= ConnectionFactory.createConnection(configuration);
        Table detailTable = connection.getTable(TableName.valueOf("detail"));
        //Table totalTable = connection.getTable(TableName.valueOf("total"));
        Admin admin=connection.getAdmin();
        List<Put> list = new ArrayList<>();
        List<Put> listTotal=new ArrayList<>();
        Put put1 = new Put(Bytes.toBytes("999967410120200209"));
        put1.addColumn(Bytes.toBytes(framliyClf), Bytes.toBytes("1"), Bytes.toBytes("123"));
        Put put2 = new Put(Bytes.toBytes("999967410120200209"));
        put2.addColumn(Bytes.toBytes(framliyClf), Bytes.toBytes("2"), Bytes.toBytes("1234"));
        Put put3 = new Put(Bytes.toBytes("999967410120200210"));
        put3.addColumn(Bytes.toBytes(framliyClf), Bytes.toBytes("2"), Bytes.toBytes("12345"));
        list.add(put1);
        list.add(put2);
        list.add(put3);
        detailTable.put(list);
        List<Get> getList=new ArrayList<>();
        Get get1=new Get(Bytes.toBytes("999967410120200209"));
        get1.addColumn(Bytes.toBytes(framliyClf), Bytes.toBytes("3"));
        Get get2=new Get(Bytes.toBytes("999967410120200210"));
        Get get3=new Get(Bytes.toBytes("999967410120200211"));
        getList.add(get1);
        getList.add(get2);
        Result[] results=detailTable.get(getList);
        for(Result result:results){
           for(Cell kv:result.rawCells()){
               String rowkey=Bytes.toString(CellUtil.cloneRow(kv));
               System.out.println(rowkey);
               String value=Bytes.toString(CellUtil.cloneValue(kv));
               System.out.println(value);
           }
        }
        //String map=getByRowKey("detail","999967410120200209","1");
/*        String line="3,2020125,商业模式前收费基金1,CNY,779.31,333.58,783.42,1014766804,844.04,1,2,3,20200209";
        String[] parts = line.split(",");
        //客户号、日期、类型、金额、总消息
        Detail detail=new Detail();
        detail.setType(parts[1]);
        detail.setMoney(new BigDecimal(parts[5]));
        detail.setCertNo(parts[7]);
        detail.setDate(parts[12]);
        detail.setAll(line);
        System.out.println(detail.getRowKey());*/
    }



    public static String getByRowKey(String tableName, String rowKey,String column) throws Exception {
        TableName name = TableName.valueOf(tableName);
        Table table = connection.getTable(name);
        Get g = new Get(rowKey.getBytes());
        g.addColumn(Bytes.toBytes(framliyClf),Bytes.toBytes(column));
        Result rs = table.get(g);
        return buildCells(rs.rawCells());
    }


    private static String buildCells(Cell[] cells) {
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
