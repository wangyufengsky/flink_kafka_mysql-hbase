package Hbase;



import entity.DetailBean;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class PutIntoHbase4PC {


    private static String framliyClf = "mainInfoCLF";
    private static int nums;


    public static void main(String[] args) {
        System.out.println("链接成功！");
        System.out.println("线程执行开始");
        long startTime=System.currentTimeMillis();
        read("D:\\big\\x4",init());
        System.out.println("插入时间："+(System.currentTimeMillis()-startTime));
        System.out.println("执行完毕**********************");
    }


    public static Table init(){
        Table detailTable =null;
        try{
            org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "node1,node2,node3,node4,node5,node6,node8,node9,node10,node11,node12,node13,node14,node15,node16,node17,node18,node19");
            configuration.set("fs.defaultFS", "hdfs://node1:9000");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily","3200");
            Connection connection= ConnectionFactory.createConnection(configuration);
            detailTable = connection.getTable(TableName.valueOf("detail"));
            Admin admin=connection.getAdmin();
        }catch (Exception e){
            e.printStackTrace();
        }
        return detailTable;
    }


    public static void put(List<DetailBean> details,Table table){
        try {
            long startTime=System.currentTimeMillis();
            List<Put> list = new ArrayList<>();
            for(DetailBean detail:details){
                Put put = new Put(Bytes.toBytes(detail.getRowKey()));
                put.addColumn(Bytes.toBytes(framliyClf), Bytes.toBytes(detail.getBusiCode()+detail.getTime()),
                        Bytes.toBytes(detail.getBusiAmount()+","+detail.getClearAmount()+","+detail.getRemark()+","+detail.getEvenId()));
                list.add(put);
            }
            long startTime1=System.currentTimeMillis();
            table.put(list);
            long endTime=System.currentTimeMillis();
            nums+=10000;
            System.out.println("已插入"+nums+"条明细数据,用时:"+(endTime-startTime)+",插入耗时:"+(endTime-startTime1));
            list.clear();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void read(String filePath,Table table){
        try(BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))) {
            List<DetailBean> detailBeans=new ArrayList<>();
            String line = null;
            while ((line = br.readLine()) != null){
                if (line.contains(",") && !line.contains("CUSTOMER_ID")) {
                    String[] str=line.replace("\"","").split(",");
                    if(str.length==8){
                        DetailBean detailBean=DetailBean.builder().custId(str[0]).busiCode(str[1]).busiAmount(str[2])
                                .clearAmount(str[3]).remark(str[4]).date(str[5]).time(str[6]).evenId(str[7]).rowKey(str[0]+str[5]).build();
                        detailBeans.add(detailBean);
                        if(detailBeans.size()>=10000){
                            put(detailBeans,table);
                            detailBeans.clear();
                        }
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }



}
