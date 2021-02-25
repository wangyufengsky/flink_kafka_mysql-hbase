package Util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

public class Tools {

    public static void main(String[] args) {
        String str="null8dcd8d7dc96d4c37be59120f049a66f1,66552141,100.0,20201019,10:59:52ConsumerRecord(topic = clear_total, partition = 26, offset = 498, CreateTime = 1603076392269, serialized key size = -1, serialized value size = 65, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 8dcd8d7dc96d4c37be59120f049a66f1,66552141,100.0,20201019,10:59:52)";
        String newStr=str.substring(str.indexOf("null")+4,str.indexOf("ConsumerRecord"));
        System.out.println(newStr);
    }

    /**
     * 获取MD5加密
     */
    public static String getMD5(String str) throws Exception {
        try{
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.update(str.getBytes());
            return new BigInteger(1,messageDigest.digest()).toString(16);
        }catch (Exception e){
            throw new Exception();
        }
    }


    /**
     * 获取o中所有的字段有值的map组合
     * @return
     */
    public static Map getFieldValue(Object o) throws IllegalAccessException {
        Map retMap = new HashMap();
        Field[] fs = o.getClass().getDeclaredFields();
        for(int i = 0;i < fs.length;i++){
            Field f = fs[i];
            f.setAccessible(true);
            if(f.get(o) != null){
                retMap.put(f.getName(),f.get(o) );
            }
        }
        return retMap;
    }

    /**
     * 通过反射,获得定义Class时声明的父类的范型参数的类型. 如public BookManager extends
     * GenricManager<Book>
     *
     * @param clazz The class to introspect
     * @return the first generic declaration, or <code>Object.class</code> if cannot be determined
     */
    public static Class getSuperClassGenricType(Class clazz) {
        return getSuperClassGenricType(clazz, 0);
    }

    /**
     * 通过反射,获得定义Class时声明的父类的范型参数的类型. 如public BookManager extends GenricManager<Book>
     *
     * @param clazz clazz The class to introspect
     * @param index the Index of the generic ddeclaration,start from 0.
     */
    public static Class getSuperClassGenricType(Class clazz, int index)
            throws IndexOutOfBoundsException {
        Type genType = clazz.getGenericSuperclass();
        if (!(genType instanceof ParameterizedType)) {
            return Object.class;
        }
        Type[] params = ((ParameterizedType) genType).getActualTypeArguments();
        if (index >= params.length || index < 0) {
            return Object.class;
        }
        if (!(params[index] instanceof Class)) {
            return Object.class;
        }
        return (Class) params[index];
    }

    public static String arraytostring(String[] strs){
        if(StringUtils.isEmpty(strs)){
            return "";
        }
        StringBuffer sb = new StringBuffer();
        Arrays.asList(strs).stream().forEach(str -> sb.append(str).append(" "));
        return sb.toString();
    }


    public static String getByRowKey(Connection connection, String tableName,String rowKey,String framliy,String column) throws Exception {
        TableName name = TableName.valueOf(tableName);
        Table table = connection.getTable(name);
        Get g = new Get(rowKey.getBytes());
        g.addColumn(Bytes.toBytes(framliy),Bytes.toBytes(column));
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

    public static <T> Predicate<T> distinctByKey(Function<? super T, Object> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>(); System.out.println("这个函数将应用到每一个item");
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }


}
