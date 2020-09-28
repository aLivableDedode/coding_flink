package functions;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import scala.Tuple3;

/**
 *  模拟一个stream 定时产生数据
 */
public class SimpleSourceFunction implements SourceFunction<Tuple3<String, Integer, Long>> {
    @Override
    public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
        int index = 1;
        while (true){
            ctx.collect(new Tuple3<>("key", ++index, System.currentTimeMillis()));
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {

    }
}
