import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Restores a ForSt async-state job alternately at parallelism 2 and 1 from native savepoints, in
 * one JVM, and prints process RSS after each run's MiniCluster is gone. Scale-in restores
 * (2 -> 1) go through ForStIncrementalRestoreOperation.restoreTempDBInstance; if its DBOptions
 * copy is never closed, the restored backend's slot-shared block cache outlives the backend.
 *
 * usage: ScaleInLeakCheck <work dir> [runs] [seconds per run]
 * run with -Xms/-Xmx equal + -XX:+AlwaysPreTouch, and jemalloc with dirty/muzzy decay 0.
 */
public class ScaleInLeakCheck {
    static final int KEYS = 400_000;

    public static class Fn extends KeyedProcessFunction<Long, Long, Long> {
        transient ValueState<byte[]> st;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext c) {
            st = getRuntimeContext().getState(new ValueStateDescriptor<>("v", byte[].class));
        }

        @Override
        public void processElement(Long v, Context ctx, Collector<Long> out) {
            st.asyncValue().thenAccept(old -> {
                byte[] b = old != null ? old : new byte[1000];
                b[0]++;
                st.asyncUpdate(b);
            });
        }
    }

    public static void main(String[] a) throws Exception {
        Path work = Path.of(a[0]);
        int runs = a.length > 1 ? Integer.parseInt(a[1]) : 6;
        int seconds = a.length > 2 ? Integer.parseInt(a[2]) : 45;
        String restoreFrom = null;
        System.out.printf("RESULT run=base rss=%d MiB%n", rssMiB());
        for (int run = 0; run < runs; run++) {
            int parallelism = run % 2 == 0 ? 2 : 1;
            Configuration c = new Configuration();
            c.set(StateBackendOptions.STATE_BACKEND, "forst");
            c.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, work.resolve("ckpt").toUri().toString());
            c.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, work.resolve("sp").toUri().toString());
            c.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
            c.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("1g"));
            c.set(TaskManagerOptions.NUM_TASK_SLOTS, 2);
            if (restoreFrom != null) c.set(StateRecoveryOptions.SAVEPOINT_PATH, restoreFrom);
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(c);
            env.setParallelism(parallelism);
            env.setMaxParallelism(128);
            env.fromSequence(0, Long.MAX_VALUE).setParallelism(1)
                    .keyBy(v -> Math.floorMod(v * 2_654_435_761L, (long) KEYS)).enableAsyncState()
                    .process(new Fn()).uid("fn")
                    .sinkTo(new DiscardingSink<>());
            JobClient job = env.executeAsync("scale-in-leak-check-" + run);
            Thread.sleep(seconds * 1000L);
            restoreFrom = job.stopWithSavepoint(false, work.resolve("sp").toUri().toString(),
                    SavepointFormatType.NATIVE).get();
            job.getJobExecutionResult().get();
            Thread.sleep(3000);  // MiniCluster shutdown
            System.gc();
            Thread.sleep(2000);
            System.out.printf("RESULT run=%d p=%d rss=%d MiB%n", run, parallelism, rssMiB());
        }
    }

    static long rssMiB() throws Exception {
        for (String l : Files.readAllLines(Path.of("/proc/self/status")))
            if (l.startsWith("VmRSS:")) return Long.parseLong(l.replaceAll("\\D", "")) / 1024;
        return -1;
    }
}
