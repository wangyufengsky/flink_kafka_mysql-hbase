package dataSoruce;


import Impl.OrderDaoImpl;
import entity.IBasicDao;
import entity.SnapshotBean;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SharedingTest {

    private static IBasicDao orderDao;


    public static IBasicDao init(){
        try {
            DataSource dataSource= DataSourceFactory.getDataSource();
            orderDao=new OrderDaoImpl(dataSource);
            System.out.println("链接成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return orderDao;
    }

    public static void main(String[] args) {
        try {
            init();
        //drop();
           //create();
           //creatIndex();
            //orderDao.dropIndex();
          //insertFormFile();
            //select();
           //orderDao.truncateTable();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static void creatIndex(){
        orderDao.creatIndex();
    }

    private static void drop(){
        orderDao.dropTable();
    }



    private static void select(){
        System.out.println(orderDao.selectCount());
    }




    private static void insertFormFile(){
        try {
            System.out.println("链接成功！");
            System.out.println("执行开始");
            long startTime=System.currentTimeMillis();
            readFile("/apps/x0");
            System.out.println("插入时间："+(System.currentTimeMillis()-startTime));
            System.out.println("执行完毕**********************");
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("插入结束");
    }

    public static void readFile(String filePath){
        try(BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))) {
            String line=null;
            List<SnapshotBean> snapshotBeans=new ArrayList<>();
            while ((line = br.readLine()) != null){
                if (line.contains(",") && !line.contains("CUSTOMER_ID")) {
                    String[] str=line.replace("\"","").split(",");
                    if(str.length==7){
                        SnapshotBean snapshotBean=SnapshotBean.builder().cst_no(Long.parseLong(str[0])).busi_type(str[1])
                                .total_bal_val(new BigDecimal(str[3])).create_date(Long.parseLong(str[5])).total_profit_val(new BigDecimal(str[6])).build();
                        snapshotBeans.add(snapshotBean);
                        if(snapshotBeans.size()>=10000){
                            orderDao.doBatch(snapshotBeans);
                            snapshotBeans.clear();
                        }
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }






}
