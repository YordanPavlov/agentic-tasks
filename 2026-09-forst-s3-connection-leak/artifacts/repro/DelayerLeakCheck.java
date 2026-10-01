import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class DelayerLeakCheck {
    static final int FAILURES = 5;
    static final AtomicInteger maxAttempt = new AtomicInteger(-1);

    public static class Fn extends KeyedProcessFunction<Long, Long, Long> {
        transient ValueState<Long> st; transient long started;
        @Override public void open(org.apache.flink.api.common.functions.OpenContext c) {
            st = getRuntimeContext().getState(new ValueStateDescriptor<>("s", Types.LONG));
            started = System.currentTimeMillis();
            maxAttempt.accumulateAndGet(getRuntimeContext().getTaskInfo().getAttemptNumber(), Math::max);
        }
        @Override public void processElement(Long v, Context ctx, Collector<Long> out) {
            st.asyncUpdate(v);
            if (getRuntimeContext().getTaskInfo().getAttemptNumber() < FAILURES
                    && getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() == 0
                    && System.currentTimeMillis() - started > 500) {
                throw new RuntimeException("induced failure");
            }
        }
    }

    public static void main(String[] a) throws Exception {
        Configuration c = new Configuration();
        c.set(StateBackendOptions.STATE_BACKEND, a.length > 0 ? a[0] : "hashmap");
        c.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        c.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 100);
        c.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(200));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(c);
        env.setParallelism(2);
        env.fromSequence(0, Long.MAX_VALUE).keyBy(v -> v % 1000).enableAsyncState()
                .process(new Fn()).sinkTo(new DiscardingSink<>());
        JobClient job = env.executeAsync("delayer-leak-check");

        long deadline = System.currentTimeMillis() + 90_000;
        while (maxAttempt.get() < FAILURES && System.currentTimeMillis() < deadline) Thread.sleep(200);
        Thread.sleep(3000);  // let the last attempt settle
        System.gc();
        Field f = Class.forName("org.apache.flink.runtime.asyncprocessing.AsyncRequestBuffer").getDeclaredField("DELAYER");
        f.setAccessible(true);
        int queued = ((ScheduledThreadPoolExecutor) f.get(null)).getQueue().size();
        System.out.println("RESULT backend=" + c.get(StateBackendOptions.STATE_BACKEND)
                + " attempts=" + (maxAttempt.get() + 1) + " DELAYER queued tasks=" + queued + " (live subtasks=2)");
        job.cancel().get();
        System.exit(0);
    }
}
