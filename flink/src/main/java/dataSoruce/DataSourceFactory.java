package dataSoruce;



import org.apache.shardingsphere.api.config.sharding.KeyGeneratorConfiguration;
import org.apache.shardingsphere.api.config.sharding.ShardingRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.TableRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.HintShardingStrategyConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.InlineShardingStrategyConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.StandardShardingStrategyConfiguration;
import org.apache.shardingsphere.shardingjdbc.api.ShardingDataSourceFactory;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DataSourceFactory {

    private static Map<String, DataSource> createDtaSourceMap(){
        Map<String,DataSource> result = new HashMap<>();
        result.put("db_0",DataSourceUtils.createDataSource("10.141.61.163","db_0"));
        result.put("db_1",DataSourceUtils.createDataSource("10.141.61.164","db_1"));
        result.put("db_2",DataSourceUtils.createDataSource("10.141.61.165","db_2"));
        result.put("db_3",DataSourceUtils.createDataSource("10.141.61.166","db_3"));
        result.put("db_4",DataSourceUtils.createDataSource("10.141.61.167","db_4"));
        result.put("db_5",DataSourceUtils.createDataSource("10.141.61.163","db_5"));
        result.put("db_6",DataSourceUtils.createDataSource("10.141.61.164","db_6"));
        result.put("db_7",DataSourceUtils.createDataSource("10.141.61.165","db_7"));
        result.put("db_8",DataSourceUtils.createDataSource("10.141.61.166","db_8"));
        result.put("db_9",DataSourceUtils.createDataSource("10.141.61.167","db_9"));
        result.put("db_10",DataSourceUtils.createDataSource("10.141.61.192","db_10"));
        result.put("db_11",DataSourceUtils.createDataSource("10.141.61.192","db_11"));
        result.put("db_12",DataSourceUtils.createDataSource("10.141.61.193","db_12"));
        result.put("db_13",DataSourceUtils.createDataSource("10.141.61.193","db_13"));
        result.put("db_14",DataSourceUtils.createDataSource("10.141.61.194","db_14"));
        result.put("db_15",DataSourceUtils.createDataSource("10.141.61.194","db_15"));
        result.put("db_16",DataSourceUtils.createDataSource("10.141.61.195","db_16"));
        result.put("db_17",DataSourceUtils.createDataSource("10.141.61.195","db_17"));
        result.put("db_18",DataSourceUtils.createDataSource("10.141.61.196","db_18"));
        result.put("db_19",DataSourceUtils.createDataSource("10.141.61.196","db_19"));
        result.put("db_20",DataSourceUtils.createDataSource("10.141.61.197","db_20"));
        result.put("db_21",DataSourceUtils.createDataSource("10.141.61.197","db_21"));
        result.put("db_22",DataSourceUtils.createDataSource("10.141.61.198","db_22"));
        result.put("db_23",DataSourceUtils.createDataSource("10.141.61.198","db_23"));
        result.put("db_24",DataSourceUtils.createDataSource("10.141.61.201","db_24"));
        result.put("db_25",DataSourceUtils.createDataSource("10.141.61.201","db_25"));
        result.put("db_26",DataSourceUtils.createDataSource("10.141.61.200","db_26"));
        result.put("db_27",DataSourceUtils.createDataSource("10.141.61.200","db_27"));
        return result;
    }

    /*public static DataSource getDataSource(String table) throws Exception{
        Map<String,DataSource> dataSourceMap=createDtaSourceMap();
        ShardingRuleConfiguration shardingRuleConfiguration=new ShardingRuleConfiguration();
        ShardingTableRuleConfiguration shardingTableRuleConfiguration=new ShardingTableRuleConfiguration(table,getDataNode(table,10));
        shardingTableRuleConfiguration.setKeyGenerateStrategy(new KeyGenerateStrategyConfiguration("id","snowflake"));
        shardingRuleConfiguration.getTables().add(shardingTableRuleConfiguration);
        shardingRuleConfiguration.setDefaultDatabaseShardingStrategy(new StandardShardingStrategyConfiguration("cst_no","database"));
        shardingRuleConfiguration.setDefaultTableShardingStrategy(new StandardShardingStrategyConfiguration("cst_no","table"));
        shardingRuleConfiguration.getKeyGenerators().put("snowflake",new ShardingSphereAlgorithmConfiguration("SNOWFLAKE",getProperties()));
        Properties tble=new Properties();
        tble.setProperty("algorithm-expression",table+"_${cst_no % 10}");
        Properties database=new Properties();
        database.setProperty("algorithm-expression","db_${(cst_no /10000) % 28}");
        shardingRuleConfiguration.getShardingAlgorithms().put("table",new ShardingSphereAlgorithmConfiguration("INLINE",tble));
        shardingRuleConfiguration.getShardingAlgorithms().put("database",new ShardingSphereAlgorithmConfiguration("INLINE",database));
        return ShardingSphereDataSourceFactory.createDataSource(dataSourceMap, Collections.singleton(shardingRuleConfiguration),new Properties());
    }*/

    public static DataSource getDataSource() throws Exception{
        Map<String,DataSource> dataSourceMap=createDtaSourceMap();
        TableRuleConfiguration tableRuleConfiguration=new TableRuleConfiguration("clear_total","db_${0..27}.clear_total_${0..10}");
        tableRuleConfiguration.setKeyGeneratorConfig(new KeyGeneratorConfiguration("SNOWFLAKE","id",getProperties()));
        tableRuleConfiguration.setDatabaseShardingStrategyConfig(new StandardShardingStrategyConfiguration("cst_no",new ShardingConfi()));
        tableRuleConfiguration.setTableShardingStrategyConfig(new InlineShardingStrategyConfiguration("cst_no","clear_total_${cst_no % 10}"));
        ShardingRuleConfiguration shardingRuleConfiguration=new ShardingRuleConfiguration();
        shardingRuleConfiguration.getTableRuleConfigs().add(tableRuleConfiguration);
        shardingRuleConfiguration.setDefaultDatabaseShardingStrategyConfig(new StandardShardingStrategyConfiguration("cst_no",new ShardingConfi()));
        shardingRuleConfiguration.setDefaultTableShardingStrategyConfig(new InlineShardingStrategyConfiguration("cst_no","clear_total_${cst_no % 10}"));
        Properties props=new Properties();
        props.setProperty("sql.show",Boolean.TRUE.toString());
        return ShardingDataSourceFactory.createDataSource(dataSourceMap,shardingRuleConfiguration,props);
    }





    private static Properties getProperties(){
        Properties result=new Properties();
        result.setProperty("worker.id", String.valueOf(getWorkId()));
        return result;
    }

    public static Long getWorkId(){
        InetAddress address = null;
        try{
            address=InetAddress.getLocalHost();

        }catch (UnknownHostException e) {
            e.printStackTrace();
        }
        byte[] ipAddressByteArray=address.getAddress();
        return  (long)(((ipAddressByteArray[ipAddressByteArray.length-2]&0B11)<<Byte.SIZE)+(ipAddressByteArray[ipAddressByteArray.length-1]&0xFF));
    }




    public static String getDataNode(String tableName,int num){
        String db0="db_0."+tableName+"_0";
        String db1=",db_1."+tableName+"_0";
        String db2=",db_2."+tableName+"_0";
        String db3=",db_3."+tableName+"_0";
        String db4=",db_4."+tableName+"_0";
        String db5=",db_5."+tableName+"_0";
        String db6=",db_6."+tableName+"_0";
        String db7=",db_7."+tableName+"_0";
        String db8=",db_8."+tableName+"_0";
        String db9=",db_9."+tableName+"_0";
        String db10=",db_10."+tableName+"_0";
        String db11=",db_11."+tableName+"_0";
        String db12=",db_12."+tableName+"_0";
        String db13=",db_13."+tableName+"_0";
        String db14=",db_14."+tableName+"_0";
        String db15=",db_15."+tableName+"_0";
        String db16=",db_16."+tableName+"_0";
        String db17=",db_17."+tableName+"_0";
        String db18=",db_18."+tableName+"_0";
        String db19=",db_19."+tableName+"_0";
        String db20=",db_20."+tableName+"_0";
        String db21=",db_21."+tableName+"_0";
        String db22=",db_22."+tableName+"_0";
        String db23=",db_23."+tableName+"_0";
        String db24=",db_24."+tableName+"_0";
        String db25=",db_25."+tableName+"_0";
        String db26=",db_26."+tableName+"_0";
        String db27=",db_27."+tableName+"_0";
        for(int j=1;j<num;j++){
            db0+=",db_0."+tableName+"_"+j;
            db1+=",db_1."+tableName+"_"+j;
            db2+=",db_2."+tableName+"_"+j;
            db3+=",db_3."+tableName+"_"+j;
            db4+=",db_4."+tableName+"_"+j;
            db5+=",db_5."+tableName+"_"+j;
            db6+=",db_6."+tableName+"_"+j;
            db7+=",db_7."+tableName+"_"+j;
            db8+=",db_8."+tableName+"_"+j;
            db9+=",db_9."+tableName+"_"+j;
            db10+=",db_10."+tableName+"_"+j;
            db11+=",db_11."+tableName+"_"+j;
            db12+=",db_12."+tableName+"_"+j;
            db13+=",db_13."+tableName+"_"+j;
            db14+=",db_14."+tableName+"_"+j;
            db15+=",db_15."+tableName+"_"+j;
            db16+=",db_16."+tableName+"_"+j;
            db17+=",db_17."+tableName+"_"+j;
            db18+=",db_18."+tableName+"_"+j;
            db19+=",db_19."+tableName+"_"+j;
            db20+=",db_20."+tableName+"_"+j;
            db21+=",db_21."+tableName+"_"+j;
            db22+=",db_22."+tableName+"_"+j;
            db23+=",db_23."+tableName+"_"+j;
            db24+=",db_24."+tableName+"_"+j;
            db25+=",db_25."+tableName+"_"+j;
            db26+=",db_26."+tableName+"_"+j;
            db27+=",db_27."+tableName+"_"+j;
        }
        return db0+db1+db2+db3+db4+db5+db6+db7+db8+db9+db10+db11+db12+db13+db14+db15+db16+db17+db18+db19+db20+db21+db22+db23+db24+db25+db26+db27;
    }


    public static void main(String[] args) {
        System.out.println(getDataNode("cst_time_point",80));
    }
}
