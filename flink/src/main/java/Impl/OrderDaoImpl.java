package Impl;


import entity.IBasicDao;
import entity.OrderBean;
import entity.SnapshotBean;
import entity.clearBean;
import org.apache.commons.collections4.CollectionUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OrderDaoImpl implements IBasicDao {

    private final DataSource dataSource ;

    private Connection connection1 = null;

    private static int num=0;

    public OrderDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
        try {
            this.connection1=this.dataSource.getConnection();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }



    /*@Override
    public void createTbaleIfNotExists() {
        String sql = "CREATE TABLE IF NOT EXISTS t_order " + "(order_id BIGINT NOT NULL AUTO_INCREMENT, user_id BIGINT NOT NULL, details VARCHAR(100), PRIMARY KEY (order_id),INDEX (user_id));";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("创建成功！");
    }*/



    @Override
    public void createTbaleIfNotExists() {
        String sql = "create table cst_time_point(" +
                " id bigint COMMENT 'id', " +
                " cst_no bigint not null COMMENT '客户号', " +
                " busi_type varchar(2) not null COMMENT '业务类型', " +
                " point_date date not null COMMENT '日期'," +
                " balance decimal(18,2) not null COMMENT '余额'," +
                " sum_amount decimal(18,2) not null COMMENT '总金额'," +
                " profit_amount decimal(18,2) not null  COMMENT '总收益'," +
                " PRIMARY KEY (id)," +
                " key idx_cst_no (cst_no) using btree," +
                " key idx_point_date (point_date) using btree " +
                " )engine=InnoDB DEFAULT CHARSET=utf8 COMMENT '资产信息表';";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("创建成功！");
    }


    public void createBalance() {
        String sql = "CREATE TABLE IF NOT EXISTS balance(" +
                " id BIGINT UNSIGNED AUTO_INCREMENT," +
                " cst_no BIGINT UNSIGNED NOT NULL," +
                " create_time TIMESTAMP NOT NULL," +
                " busi_type VARCHAR(4) NOT NULL," +
                " bal_val DECIMAL(18,2) NOT NULL," +
                " key idx_cst_no (cst_no) using btree," +
                " key idx_create_time (create_time) using btree " +
                "PRIMARY KEY (id)" +
                ")ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '余额表';";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("创建成功！");
    }

    public void createProfit() {
        String sql = "CREATE TABLE IF NOT EXISTS profit(" +
                " id BIGINT UNSIGNED AUTO_INCREMENT," +
                " cst_no BIGINT UNSIGNED NOT NULL," +
                " create_time TIMESTAMP NOT NULL," +
                " busi_type VARCHAR(4) NOT NULL," +
                " profit_val DECIMAL(18,2) NOT NULL," +
                " key idx_cst_no (cst_no) using btree," +
                " key idx_create_time (create_time) using btree " +
                "PRIMARY KEY (id)" +
                ")ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '收益表';";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("创建成功！");
    }

    public void createTotalBalance() {
        String sql = "CREATE TABLE IF NOT EXISTS total_balance(" +
                " id BIGINT UNSIGNED AUTO_INCREMENT," +
                " cst_no BIGINT UNSIGNED NOT NULL," +
                " create_time TIMESTAMP NOT NULL," +
                " total_bal_val DECIMAL(18,2) NOT NULL," +
                " key idx_cst_no (cst_no) using btree," +
                " key idx_create_time (create_time) using btree " +
                "PRIMARY KEY (id)" +
                ")ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '总余额表';";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("创建成功！");
    }

    public void createTotalProfit() {
        String sql = "CREATE TABLE IF NOT EXISTS total_profit(" +
                " id BIGINT UNSIGNED AUTO_INCREMENT," +
                " cst_no BIGINT UNSIGNED NOT NULL," +
                " create_time TIMESTAMP NOT NULL," +
                " total_profit_val DECIMAL(18,2) NOT NULL," +
                " key idx_cst_no (cst_no) using btree," +
                " key idx_create_time (create_time) using btree " +
                "PRIMARY KEY (id)" +
                ")ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '总收益表';";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("创建成功！");
    }

    @Override
    public void createSnapshot() {
        String sql = "CREATE TABLE IF NOT EXISTS snapshot(" +
                " id BIGINT AUTO_INCREMENT," +
                " cst_no BIGINT UNSIGNED NOT NULL," +
                " create_date BIGINT NOT NULL," +
                " busi_type VARCHAR(4) NOT NULL," +
                " total_bal_val DECIMAL(18,2) NOT NULL," +
                " total_profit_val DECIMAL(18,2) NOT NULL," +
                /*" key idx_cst_no (cst_no) using btree," +
                " key idx_create_date (create_date) using btree," +*/
                "PRIMARY KEY (id)" +
                ")ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT '快照表';";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("创建成功！");
    }


    @Override
    public void creatIndex() {
        String sql = "CREATE INDEX `snapshot_idx1` ON `snapshot`(`cst_no`,`create_date`);";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("创建成功！");
    }

    @Override
    public void dropIndex() {
        String sql = "drop index `snapshot_idx1` ON `snapshot`;";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("创建成功！");
    }

    @Override
    public void repair() {
        String sql = "REPAIR TABLE `snapshot`;";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("创建成功！");
    }


    @Override
    public void dropTable() {
        String sql = "DROP TABLE IF EXISTS snapshot";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("删除成功！");
    }

    @Override
    public void truncateTable() {
        String sql = "TRUNCATE TABLE snapshot";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("TRUNCATE成功！");
    }




    @Override
    public List<OrderBean> select() {
        String sql = "SELECT COUNT(*) FROM t_order";
        return listOrders(sql);
    }


    @Override
    public String selectCount() {
        String sql = "SELECT COUNT(*) FROM t_order";
        return listCount(sql);
    }

    @Override
    public void batchInsert(List<SnapshotBean> order) {
        List<List<SnapshotBean>> newOrders=splitList(order,10000);
        newOrders.forEach(this::doBatch);
    }

    @Override
    public void clearInsert(List<clearBean> order) {
        List<List<clearBean>> newOrders=splitList(order,10000);
        newOrders.forEach(this::doBatchClear);
    }

    @Override
    public void doBatchClear(List<clearBean> order){
        long startTime=System.currentTimeMillis();
        StringBuffer sql=new StringBuffer("INSERT INTO clear_total (event_id,cst_no,create_date,create_time,clear_total) VALUES ");
        int times=0;
        try (Connection connection = dataSource.getConnection()) {
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
            System.out.println("已插入"+num+"笔,10000笔插入耗时："+(System.currentTimeMillis()-startTime1)+"，运行总耗时："+(System.currentTimeMillis()-startTime));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void InsertSnapshot(List<SnapshotBean> order) {
        List<List<SnapshotBean>> newOrders=splitList(order,10000);
        for(List<SnapshotBean> beans:newOrders){
            Thread t=new Thread(() -> {
                try {
                    doBatchSnapshotBean(beans);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            t.start();
        }
    }




    @Override
    public void doSingleSnapshotBean(List<SnapshotBean> order){
        long startTime=System.currentTimeMillis();
        String sql = "INSERT INTO snapshot (cst_no,create_date,busi_type,total_bal_val,total_profit_val) VALUES (?,?,?,?,?)";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for(SnapshotBean bean:order){
                preparedStatement.setLong(1, bean.getCst_no());
                preparedStatement.setLong(2, bean.getCreate_date());
                preparedStatement.setString(3, bean.getBusi_type());
                preparedStatement.setBigDecimal(4, bean.getTotal_bal_val());
                preparedStatement.setBigDecimal(5, bean.getTotal_profit_val());
                preparedStatement.execute();
            }
            num+=50000;
            System.out.println("已插入"+num+"笔,50000笔插入耗时："+(System.currentTimeMillis()-startTime));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }





    @Override
    public void doBatchSnapshotBean(List<SnapshotBean> order){
        long startTime=System.currentTimeMillis();
        String sql = "INSERT INTO snapshot (cst_no,create_date,busi_type,total_bal_val,total_profit_val) VALUES (?,?,?,?,?)";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for(SnapshotBean bean:order){
                preparedStatement.setLong(1, bean.getCst_no());
                preparedStatement.setLong(2, bean.getCreate_date());
                preparedStatement.setString(3, bean.getBusi_type());
                preparedStatement.setBigDecimal(4, bean.getTotal_bal_val());
                preparedStatement.setBigDecimal(5, bean.getTotal_profit_val());
                preparedStatement.addBatch();
            }
            long startTime1=System.currentTimeMillis();
            preparedStatement.executeBatch();
            num+=50000;
            System.out.println("已插入"+num+"笔,50000笔插入耗时："+(System.currentTimeMillis()-startTime1)+"，运行总耗时："+(System.currentTimeMillis()-startTime));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }







    @Override
    public void doBatch(List<SnapshotBean> order){
        long startTime=System.currentTimeMillis();
        StringBuffer sql=new StringBuffer("INSERT INTO snapshot (cst_no,create_date,busi_type,total_bal_val,total_profit_val) VALUES ");
        int times=0;
        try (Connection connection = dataSource.getConnection()) {
            for(SnapshotBean bean:order){
                sql.append("(").append(bean.getCst_no()).append(",").append(bean.getCreate_date()).append(",").append(bean.getBusi_type()).append(",").append(bean.getTotal_bal_val()).append(",").append(bean.getTotal_profit_val()).append(")");
                times++;
                if(times!=10000){
                    sql.append(",");
                }
            }
            long startTime1 = System.currentTimeMillis();
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql.toString())) {
                preparedStatement.addBatch();
                preparedStatement.executeBatch();
            }
            num+=10000;
            System.out.println("已插入"+num+"笔,10000笔插入耗时："+(System.currentTimeMillis()-startTime1)+"，运行总耗时："+(System.currentTimeMillis()-startTime));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List selectByUserId(Long id) {
        String sql = "SELECT * FROM t_order where user_id="+id+"";
        return listOrders(sql);
    }

    @Override
    public void insert(List<OrderBean> orders) {
        String sql = "INSERT INTO t_order (user_id, details) VALUES (?, ?)";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for(OrderBean bean:orders){
                preparedStatement.setLong(1, bean.getUserId());
                preparedStatement.setString(2, bean.getDetails());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            System.out.println("批量成功！");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }




    protected List<OrderBean> listOrders(String sql) {
        List<OrderBean> result = new LinkedList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                OrderBean order = new OrderBean();
                order.setOrderId(resultSet.getLong(1));
                order.setUserId(resultSet.getLong(2));
                order.setDetails(resultSet.getString(3));
                result.add(order);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }


    protected String listCount(String sql) {
        String result=null;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            result=resultSet.getString(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public void update(OrderBean order) {
        String sql = "UPDATE t_order SET user_id=?, details=? WHERE order_id=?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setLong(1, order.getUserId());
            preparedStatement.setString(2, order.getDetails());
            preparedStatement.setLong(3, order.getOrderId());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delete(Long id) {
        String sql = "DELETE FROM t_order WHERE order_id=?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private static <T> List<List<T>> splitList(List<T> list,int length){
        if(CollectionUtils.isEmpty(list)){
            return Collections.emptyList();
        }
        int maxSize=(list.size()+length-1)/length;
        return Stream.iterate(0,n->n+1)
                .limit(maxSize)
                .parallel()
                .map(a->list.parallelStream().skip(a*length).limit(length).collect(Collectors.toList()))
                .filter(b->!b.isEmpty())
                .collect(Collectors.toList());
    }


}
