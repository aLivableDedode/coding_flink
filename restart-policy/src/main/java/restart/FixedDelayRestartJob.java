package restart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/*
 * @Author lvkai
 * @Description
 * @Date 2020/8/30 23:05
 **/
public class FixedDelayRestartJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 尝试重启三次 每次间隔2s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS)));

        DataStreamSource<Tuple3<String, Integer, Long>> streamSource = env.addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
            @Override
            public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
                int index = 1;
                while (true) {
                    ctx.collect(new Tuple3<>("key", index++, System.currentTimeMillis()));
                    Thread.sleep(100);
                }
            }

            @Override
            public void cancel() {

            }
        });

        streamSource.map(new MapFunction<Tuple3<String, Integer, Long>, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple3<String, Integer, Long> event) throws Exception {
                if (event.f1 % 20 == 0) {
                    String format = String.format("Bad data [%d] ...", event.f1);
                    System.out.println(format);
                    throw new RuntimeException(format);
                }
                return Tuple2.of(event.f0,event.f1);
            }
        }).print();
        env.execute("FixedDelayRestartJob");
    }
}
