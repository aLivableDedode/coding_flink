import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

/**
 * @Author lvkai
 * @Description  开启了chp机制后，有时我们希望在Flink运行时或运行中断后ckp得状态能保存下来
 *
 * @Date 2020/9/5 23:53
 **/
public class KeepCheckpointForRestore {
    public static void main(String[] args) throws Exception {
        Logger log = LoggerFactory.getLogger(EnableCheckpointRestartJob.class);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS)));
        // 开启ckp机制
        env.enableCheckpointing(200);
        // 指定ckp保存得路径
        env.setStateBackend(new FsStateBackend("file:///KeepCheckpointForRestore"));

        // 停止作业保留CP文件
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        DataStream<Tuple3<String, Integer, Long>> source = env
                .addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
                    @Override
                    public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
                        int index = 1;
                        while(true){
                            ctx.collect(new Tuple3<>("key", index++, System.currentTimeMillis()));
                            Thread.sleep(100);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                });
        source.map(new MapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(Tuple3<String, Integer, Long> event) throws Exception {
                if(event.f1 % 30 == 0) {
                    String msg = String.format("Bad data [%d]...", event.f1);
                    log.error(msg);
                    // 抛出异常，作业根据 配置 的重启策略进行恢复，无重启策略作业直接退出。
                    throw new RuntimeException(msg);
                }
                return new Tuple3<>(event.f0, event.f1, new Timestamp(System.currentTimeMillis()).toString());
            }
        }).print();

        env.execute("KeepCheckpointForRestore");
    }
}
