package Hbase;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;


import java.util.List;

public class HbaseSink extends RichSinkFunction<List<Put>> {
    private org.apache.hadoop.conf.Configuration configuration;
    private Connection connection = null;
    private BufferedMutator userMutator;
    private BufferedMutator scencesMutator;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        configuration = HBaseConfiguration.create();
//                        configuration.set("hbase.master", "node1.hadoop:60020");
        configuration.set("hbase.zookeeper.quorum", "node1.hadoop,node2.hadoop,node3.hadoop");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("wxgz_user_data_test"));
        params.writeBufferSize(1024 * 1024);
        userMutator = connection.getBufferedMutator(params);

        BufferedMutatorParams params2 = new BufferedMutatorParams(TableName.valueOf("wxgz_scenes_data_test2"));
        params2.writeBufferSize(1024 * 1024);
        scencesMutator = connection.getBufferedMutator(params2);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //todo 关闭连接

        scencesMutator.close();
        connection.close();
    }


    @Override
    public void invoke(List<Put> value) throws Exception {
        //todo 存储
        if (value.size() > 0) {
            scencesMutator.mutate(value);
            scencesMutator.flush();
            value.clear();
        }
    }
}
