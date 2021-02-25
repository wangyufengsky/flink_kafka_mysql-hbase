package dataSoruce;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;

public class DataSourceUtils {

    public static DataSource createDataSource(final String ip,final String dataSourceName){
        DruidDataSource dataSource=new DruidDataSource();
        String jdbcUrl=String.format("jdbc:mysql://%s:3306/%s?useUnicode=true&characterEncoding=utf8&useSSL=false&rewriteBatchedStatements=true",ip,dataSourceName);
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        dataSource.setMaxWait(-1);
        dataSource.setMaxActive(1000);
        dataSource.setInitialSize(2);
        dataSource.setKeepAlive(true);
        return dataSource;
    }
}
