package Hbase;

import com.alibaba.druid.pool.DruidDataSource;
import entity.DataBaseTableBean;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

public class repairTable {

    private static Map<String, Connection> connectionMap=new HashMap<>();
    private static Map<String, String> tableMap=new HashMap<>();
    private static List<DataBaseTableBean> dataBaseTableBeans=new ArrayList<>();


    public static void main(String[] args) {
        /*for(int i=0;i<28;i++){
            int finalI = i;
            Thread t=new Thread(()-> repair(finalI,"balance"));
            t.start();
        }*/
        for(int i=0;i<28;i++){
            int finalI = i;
            Thread t=new Thread(()-> createIndex(finalI,"clear"));
            t.start();
        }
    }



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
        tableMap.put("0","clear_0");
        tableMap.put("1","clear_1");
        tableMap.put("2","clear_2");
        tableMap.put("3","clear_3");
        tableMap.put("4","clear_4");
        tableMap.put("5","clear_5");
        tableMap.put("6","clear_6");
        tableMap.put("7","clear_7");
        tableMap.put("8","clear_8");
        tableMap.put("9","clear_9");
    }


    private static void createBeans(){
        for(int i=0;i<28;i++){
            for(int j=0;j<10;j++){
                dataBaseTableBeans.add(new DataBaseTableBean(String.valueOf(i),String.valueOf(j)));
            }
        }
    }
    
    


    public static void open() {
        try {
            createTableMap();
            createBeans();
            connectionMap=createDtaSourceMap();
            System.out.println("已连接！");
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static void close() {
        connectionMap.values().forEach(s-> {
            try {
                if(!s.isClosed()){
                    s.close();
                }
            } catch (Exception throwables) {
                throwables.printStackTrace();
            }
        });
        System.out.println("已关闭！");
    }


    private static void repair(int data,String table){
        try (Connection connection=createDtaSourceMap().get(String.valueOf(data));
             Statement statement=connection.createStatement()){
            for(int i=0;i<10;i++){
                String sql=String.format("repair table %s_%s",table,i);
                statement.addBatch(sql);
            }
            statement.executeBatch();
            System.out.println("已经修复数据库db_"+data);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    private static void createIndex(int data,String table){
        try (Connection connection=createDtaSourceMap().get(String.valueOf(data));
             Statement statement=connection.createStatement()){
            for(int i=0;i<10;i++){
                String sql=String.format("CREATE INDEX `idx2` ON `%s_%s`(`cst_no`,`create_date`,`busi_code`)",table,i);
                statement.addBatch(sql);
            }
            statement.executeBatch();
            System.out.println("已经创建索引数据库db_"+data);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
