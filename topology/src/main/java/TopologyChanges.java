import functions.SimpleSourceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;

/**
 *  拓扑结构变化 需求
 */
public class TopologyChanges {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(200);

        //定义输出
        OutputTag<Tuple3<String, Integer, Long>> outputTag =
                new OutputTag<Tuple3<String, Integer, Long>>("side-output") {};
        // 最初始的版本
        //versionV1(env,outputTag);
    }

    public static void versionV1(StreamExecutionEnvironment env,OutputTag<Tuple3<String, Integer, Long>> outputTag){
        env.addSource(new SimpleSourceFunction())
                .keyBy(0).sum(1);
    }
}
