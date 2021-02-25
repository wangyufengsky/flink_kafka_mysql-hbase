package Hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


public class BulkDatas extends Configured implements Tool {
    // 输入key的类型 输入value的类型 输出key的类型 输出value的类型
    public static class ReadDatasMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        // 输出结果
        private Text inputKey = new Text();
        private final String[] nameTable = { "rowkey", "name", "age", "addr", "addrs" };

        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] world = line.split(",");
            Put put = new Put(Bytes.toBytes(world[0]));
            ImmutableBytesWritable mapOutput = new ImmutableBytesWritable(Bytes.toBytes(world[0]));
            for (int i = 0; i < world.length; i++) {
                inputKey.set(nameTable[i]);
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(nameTable[i]), Bytes.toBytes(world[i]));
            }
            context.write(mapOutput, put);

        }

    }

    /**
     * 不需要写reducer方法，我们不对数据做任何的计算操作
     */

    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop01.com");
        Job job = Job.getInstance(conf, "hbase-bulkload");

        job.setJarByClass(BulkDatas.class);

// set input path
        FileInputFormat.setInputPaths(job, new Path(args[1]));

// set map
        job.setMapperClass(ReadDatasMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        HTable table = new HTable(conf, args[0]);

        HFileOutputFormat2.configureIncrementalLoad(job, table);

// job是否提交成功
        boolean res = job.waitForCompletion(true);

// 将Hfile导入到hbase表中 相当于shell中的 completebulkload
        LoadIncrementalHFiles load = new LoadIncrementalHFiles(conf);
        load.doBulkLoad(new Path(args[2]), table);

        return res ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        args = new String[] { "stu_import", "hdfs://hadoop01.com:8020/importTSV/stu_out",
                "hdfs://hadoop01.com:8020/testHfile" };
        int status = new BulkDatas().run(args);
        System.exit(status);
    }

}


