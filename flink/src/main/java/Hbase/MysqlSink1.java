package Hbase;


import dataSoruce.DataSourceFactory;
import entity.clearBean;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MysqlSink1 extends RichSinkFunction<List<clearBean>> implements Serializable {


    private static Connection connection;
    private static  DataSource dataSource;
    private static int num=0;
    private static List<clearBean> list=new ArrayList<>();


    public static void init(){
        try {
            dataSource = DataSourceFactory.getDataSource();
            connection = dataSource.getConnection();
            System.out.println("链接成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void open(Configuration parameters) {
        try {
            super.open(parameters);
            init();
        }catch (Exception e){
            e.printStackTrace();
        }
    }



    @Override
    public void close() throws Exception {
        if(!connection.isClosed()){
            connection.close();
            System.out.println("已关闭！");
        }
        super.close();
    }



    @Override
    public void invoke(List<clearBean> value, Context context) {
        list.addAll(value);
        List<List<clearBean>> lists=splitList(list,10000);
        list.clear();
        for(List<clearBean> beans:lists){
            if(beans.size()==10000){
                doBatchClear(beans);
            }else {
                list.addAll(beans);
            }
        }
    }


    public void doBatchClear(List<clearBean> order){
        long startTime=System.currentTimeMillis();
        StringBuffer sql=new StringBuffer("INSERT INTO clear_total (event_id,cst_no,create_date,create_time,clear_total) VALUES ");
        int times=0;
        for(clearBean bean:order){
            sql.append("('").append(bean.getEvent_no()).append("',").append(bean.getCst_no()).append(",'").append(bean.getDate()).append("','").append(bean.getTime()).append("',").append(bean.getClear_total()).append(")");
            times++;
            if(times!=10000){
                sql.append(",");
            }
        }
        long startTime1 = System.currentTimeMillis();
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql.toString())) {
            preparedStatement.execute();
        }catch (Exception e){
            System.out.println("1");
            e.printStackTrace();
        }
        num+=10000;
        System.out.println("已插入"+num+"笔,10000："+(System.currentTimeMillis()-startTime1)+"，运行总耗时："+(System.currentTimeMillis()-startTime));
    }


    public void doBatchSnapshotBean(List<clearBean> order){
        long startTime=System.currentTimeMillis();
        String sql = "INSERT INTO clear_total (event_id,cst_no,create_date,create_time,clear_total) VALUES (?,?,?,?,?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for(clearBean bean:order){
                preparedStatement.setString(1, bean.getEvent_no());
                preparedStatement.setLong(2, bean.getCst_no());
                preparedStatement.setString(3, bean.getDate());
                preparedStatement.setString(4, bean.getTime());
                preparedStatement.setBigDecimal(5, bean.getClear_total());
                preparedStatement.addBatch();
            }
            long startTime1=System.currentTimeMillis();
            preparedStatement.executeBatch();
            num+=10000;
            System.out.println("已插入"+num+"笔,10000笔插入耗时："+(System.currentTimeMillis()-startTime1)+"，运行总耗时："+(System.currentTimeMillis()-startTime));
        } catch (Exception e) {
            e.printStackTrace();
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



}
