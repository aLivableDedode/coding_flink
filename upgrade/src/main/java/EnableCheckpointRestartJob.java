import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * @Author lvkai
 * @Description
 *      对任务开启checkpoint机制但是之中checkpoint产生得文件在任务结束得时候会消失
 *         另外这个ckp得状态也是只是在任务运行过程中才生效，单纯得重启并不会生效
 * @Date 2020/9/5 23:33
 **/
public class EnableCheckpointRestartJob {
    public static void main(String[] args) throws Exception {
        Logger log = LoggerFactory.getLogger(EnableCheckpointRestartJob.class);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(200);

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

        env.execute("EnableCheckpointRestartJob");
    }
}
