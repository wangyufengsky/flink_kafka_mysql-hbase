package RelTest.Sink;

import RelTest.InsertBean.BalanceTotalInsertBean;
import com.alibaba.druid.pool.DruidDataSource;
import entity.DataBaseTableBean;
import entity.clearBean;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BalanceTotalSink extends RichSinkFunction<List<clearBean>> implements Serializable {

    private static int num=0;
    private static List<clearBean> list=new ArrayList<>();
    private static Map<String, Connection> connectionMap=new HashMap<>();
    private static Map<String, String> tableMap=new HashMap<>();
    private static List<BalanceTotalInsertBean> balanceTotalBeans =new ArrayList<>();
    private static final long allStartTime=System.currentTimeMillis();

    public static Connection createConnection(final String ip, final String dataSourceName){
        DruidDataSource dataSource=new DruidDataSource();
        String jdbcUrl=String.format("jdbc:mysql://%s:3306/%s?useUnicode=true&characterEncoding=utf8&useSSL=false&rewriteBatchedStatements=true",ip,dataSourceName);
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        dataSource.setMaxWait(-1);
        dataSource.setMaxActive(1000);
        dataSource.setInitialSize(0);
        dataSource.setKeepAlive(true);
        Connection connection=null;
        try {
            connection=dataSource.getConnection();
        } catch (Exception throwables) {
            throwables.printStackTrace();
        }
        return connection;
    }
    
    
    private static Map<String, Connection> createDtaSourceMap(){
        Map<String,Connection> result = new HashMap<>();
        result.put("0", createConnection("10.141.61.163","db_0"));
        result.put("1", createConnection("10.141.61.164","db_1"));
        result.put("2", createConnection("10.141.61.165","db_2"));
        result.put("3", createConnection("10.141.61.166","db_3"));
        result.put("4", createConnection("10.141.61.167","db_4"));
        result.put("5", createConnection("10.141.61.163","db_5"));
        result.put("6", createConnection("10.141.61.164","db_6"));
        result.put("7", createConnection("10.141.61.165","db_7"));
        result.put("8", createConnection("10.141.61.166","db_8"));
        result.put("9", createConnection("10.141.61.167","db_9"));
        result.put("10", createConnection("10.141.61.192","db_10"));
        result.put("11", createConnection("10.141.61.192","db_11"));
        result.put("12", createConnection("10.141.61.193","db_12"));
        result.put("13", createConnection("10.141.61.193","db_13"));
        result.put("14", createConnection("10.141.61.194","db_14"));
        result.put("15", createConnection("10.141.61.194","db_15"));
        result.put("16", createConnection("10.141.61.195","db_16"));
        result.put("17", createConnection("10.141.61.195","db_17"));
        result.put("18", createConnection("10.141.61.196","db_18"));
        result.put("19", createConnection("10.141.61.196","db_19"));
        result.put("20", createConnection("10.141.61.197","db_20"));
        result.put("21", createConnection("10.141.61.197","db_21"));
        result.put("22", createConnection("10.141.61.198","db_22"));
        result.put("23", createConnection("10.141.61.198","db_23"));
        result.put("24", createConnection("10.141.61.201","db_24"));
        result.put("25", createConnection("10.141.61.201","db_25"));
        result.put("26", createConnection("10.141.61.200","db_26"));
        result.put("27", createConnection("10.141.61.200","db_27"));
        return result;
    }

    private static void createTableMap(){
        tableMap.put("0","balance_total_0");
        tableMap.put("1","balance_total_1");
        tableMap.put("2","balance_total_2");
        tableMap.put("3","balance_total_3");
        tableMap.put("4","balance_total_4");
        tableMap.put("5","balance_total_5");
        tableMap.put("6","balance_total_6");
        tableMap.put("7","balance_total_7");
        tableMap.put("8","balance_total_8");
        tableMap.put("9","balance_total_9");
    }


    private static void createBeans(){
        for(int i=0;i<28;i++){
            for(int j=0;j<10;j++){
                balanceTotalBeans.add(new BalanceTotalInsertBean(String.valueOf(i),String.valueOf(j)));
            }
        }
    }
    
    

    @Override
    public void open(Configuration parameters) {
        try {
            super.open(parameters);
            createTableMap();
            createBeans();
            connectionMap=createDtaSourceMap();
            System.out.println("已连接！");
        }catch (Exception e){
            e.printStackTrace();
        }
    }



    @Override
    public void close() throws Exception {
        if(!list.isEmpty()){
            doDeal(list);
            list.clear();
        }
        connectionMap.values().forEach(s-> {
            try {
                if(!s.isClosed()){
                    s.close();
                }
            } catch (Exception throwables) {
                throwables.printStackTrace();
            }
        });
        super.close();
        System.out.println("已关闭！");
    }



    @Override
    public void invoke(List<clearBean> value, Context context) throws Exception {
        list.addAll(value);
        if(list.size()>=1){
            Thread.sleep(1000);
            doDeal(list);
            list.clear();
        }
    }





    private static <T> List<List<T>> splitList(List<T> list,int length){
        if(CollectionUtils.isEmpty(list)){
            return Collections.emptyList();
        }
        int maxSize=(list.size()+length-1)/length;
        return Stream.iterate(0, n->n+1)
                .limit(maxSize)
                .parallel()
                .map(a->list.parallelStream().skip(a*length).limit(length).collect(Collectors.toList()))
                .filter(b->!b.isEmpty())
                .collect(Collectors.toList());
    }
    
    
    
    private void doDeal(List<clearBean> list){
        long startTime=System.currentTimeMillis();
        for(clearBean bean:list){
            for(BalanceTotalInsertBean dataBaseTableBean: balanceTotalBeans){
                if(dataBaseTableBean.isBelong(bean)){
                    dataBaseTableBean.addBean(bean);
                }
            }
        }
        Map<String,List<BalanceTotalInsertBean>> map= balanceTotalBeans.stream().parallel().collect(Collectors.groupingBy(BalanceTotalInsertBean::getDataBase));
        for(Map.Entry<String,List<BalanceTotalInsertBean>> entry:map.entrySet()){
            dataBaseInsert(entry.getValue(),entry.getKey());
        }
        for(BalanceTotalInsertBean dataBaseTableBean: balanceTotalBeans){
            num+=dataBaseTableBean.getClearBeans().size();
            dataBaseTableBean.clear();
        }
        System.out.println("已插入"+num+"笔,耗时："+(System.currentTimeMillis()-startTime)+"运行总耗时："+(System.currentTimeMillis()-allStartTime));
    }
    
    private void dataBaseInsert(List<BalanceTotalInsertBean> list,String data){
        int i=0;
        for(BalanceTotalInsertBean s:list){
            i+=s.getClearBeans().size();
        }
        if(i==0){
            return;
        }
        Connection connection=connectionMap.get(data);
        Map<String,List<BalanceTotalInsertBean>> map=list.stream().parallel().collect(Collectors.groupingBy(BalanceTotalInsertBean::getTableName));
        try (Statement statement = connection.createStatement()) {
            for(Map.Entry<String,List<BalanceTotalInsertBean>> entry:map.entrySet()){
                for (BalanceTotalInsertBean databean:entry.getValue()){
                    List<clearBean> beans=databean.getClearBeans();
                    if(beans.size()==0){
                        return;
                    }
                    StringBuffer sql=new StringBuffer("INSERT INTO ").append(entry.getKey()).append(" (event_id,cst_no,create_date,create_time,balance_total) VALUES ");
                    for(clearBean bean:beans){
                        sql.append("('").append(bean.getEvent_no()).append("',").append(bean.getCst_no()).append(",'").append(bean.getDate()).append("','").append(bean.getTime()).append("',").append(bean.getClear_total()).append(")");
                    }
                    System.out.println(sql);
                    statement.addBatch(sql.toString());
                }
            }
            statement.executeBatch();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    



    private void doBatchClear(DataBaseTableBean dataBaseTableBean){
        List<clearBean> beans=dataBaseTableBean.getClearBeans();
        StringBuffer sql=new StringBuffer("INSERT INTO ").append(dataBaseTableBean.getTableName()).append(" (event_id,cst_no,create_date,create_time,clear_total) VALUES ");
        int times=0;
        for(clearBean bean:beans){
            sql.append("('").append(bean.getEvent_no()).append("',").append(bean.getCst_no()).append(",'").append(bean.getDate()).append("','").append(bean.getTime()).append("',").append(bean.getClear_total()).append(")");
            times++;
            if(times!=beans.size()){
                sql.append(",");
            }
        }
        Connection connection=connectionMap.get(dataBaseTableBean.getDataBase());
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql.toString())) {
            preparedStatement.execute();
            /*preparedStatement.addBatch(sql.toString());
            preparedStatement.executeBatch();*/
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
