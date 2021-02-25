package dataSoruce;


import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;

import java.util.Collection;

public class ShardingConfi1 implements PreciseShardingAlgorithm<Integer> {


    @Override
    public String doSharding(Collection<String> collection, PreciseShardingValue<Integer> preciseShardingValue) {
        int cstNo=preciseShardingValue.getValue()%10;
        for(String each:collection){
            if(each.substring(each.length()-1).equals(String.valueOf(cstNo))){
                return each;
            }
        }
        throw new IllegalArgumentException();
    }
}
