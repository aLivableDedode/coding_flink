package main.demandfuncs;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.management.ValueExp;
import java.util.Locale;
import java.util.TimeZone;

/**
 * 每小时计算一次，每天24点清除状态
 *  --> 模拟：每5s计算一次，每20s清除状态
 */
public class ClearStateWhen24 {

    private static Logger logger = LoggerFactory.getLogger(ClearStateWhen24.class);
    protected static FastDateFormat dateFormat =
            FastDateFormat.getInstance("HH:mm:ss", TimeZone.getDefault(), Locale.getDefault());

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 定义输出
        OutputTag<Tuple2<String,Long>> outputTag = new OutputTag<Tuple2<String,Long>>("sum-output"){};

        DataStreamSource<Tuple3<String, Integer, Long>> dataStreamSource = env.addSource(new MockSource());
        SingleOutputStreamOperator<String> outputStreamOperator = dataStreamSource
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>>() {
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(System.currentTimeMillis() - 5000l);
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element, long previousElementTimestamp) {
                        return element.f2;
                    }
                }).keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple3<String, Integer, Long>, String>() {
                    private transient ValueState<Long> sumState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        logger.info("open初始化...");
                        sumState = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", Long.class));
                    }

                    @Override
                    public void processElement(Tuple3<String, Integer, Long> dataTuple2, Context context, Collector<String> collector) throws Exception {
                        Long currentValue = sumState.value();
                        if (null == currentValue) {
                            logger.info("currentValue Initialize ......");
                            currentValue = 0l;
                        }

                        currentValue += dataTuple2.f1;

                        sumState.update(currentValue);

                        logger.info("currProTime :" + dateFormat.format(context.timerService().currentProcessingTime())
                        +" 当前时间 :"+dateFormat.format(System.currentTimeMillis()));
                        // context.timerService().registerProcessingTimeTimer(dataTuple2.f1 + 5000);
                        context.timerService().registerEventTimeTimer(dataTuple2.f1 + 5000);
                        collector.collect("当前currentValue : " + currentValue);
                        // context.output(outputTag, Tuple2.of(dateFormat.format(System.currentTimeMillis()), currentValue));
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        logger.info("onTimer-->" + dateFormat.format(timestamp) + " Watermark -->"
                        +dateFormat.format(ctx.timerService().currentWatermark()));
                        Long value = sumState.value();

                        logger.info("onTimer--sumState_value :" + value);
                        //sumState.clear();

                    }
                }).uid("clear-state-when24");
        outputStreamOperator.print().setParallelism(1);
        env.execute("ClearStateWhen24");

    }

    public static class MockSource implements SourceFunction<Tuple3<String,Integer, Long>>{
        public void run(SourceContext<Tuple3<String,Integer, Long>> ctx) throws Exception {
            int value = 1;
            while (true){
                long currentTimeMillis = System.currentTimeMillis();
                ctx.collect(Tuple3.of("key",value,currentTimeMillis));
                logger.info("===> 生成时间"+dateFormat.format(currentTimeMillis));
                Thread.sleep(1000);
            }
        }

        public void cancel() {

        }
    }
}
