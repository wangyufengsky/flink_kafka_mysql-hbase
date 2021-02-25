package dataSoruce;


import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;

import java.util.Collection;

public class ShardingConfi implements PreciseShardingAlgorithm<Integer> {


    @Override
    public String doSharding(Collection<String> collection, PreciseShardingValue<Integer> preciseShardingValue) {
        int cstNo=(preciseShardingValue.getValue()/10000)%28;
        for(String each:collection){
            if(each.split("_")[1].equals(String.valueOf(cstNo))){
                return each;
            }
        }
        throw new IllegalArgumentException();
    }
}
