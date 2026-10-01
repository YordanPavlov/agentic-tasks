import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.state.forst.fs.cache.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Queue;

public class LeakCheck {
    static class Tracking extends FSDataInputStream {
        boolean closed; boolean failOnClose; long pos;
        Tracking(boolean f) { failOnClose = f; }
        public void seek(long p) { pos = p; }
        public long getPos() { return pos; }
        public int read() { return -1; }
        public void close() throws IOException { closed = true; if (failOnClose) throw new IOException("boom"); }
    }

    public static void main(String[] a) throws Exception {
        Path base = new Path(Files.createTempDirectory("forst-cache").toUri());
        FileBasedCache cache = new FileBasedCache(new Configuration(),
                new SizeBasedCacheLimitPolicy(1 << 20, 0), FileSystem.getLocalFileSystem(), base, null);
        Path remote = new Path("s3://bucket/job/000123.sst");
        cache.registerInCache(remote, 10);
        FileCacheEntry entry = cache.get(new Path(base, remote.getName()).toString(), false);
        java.lang.reflect.Field f = FileCacheEntry.class.getDeclaredField("openedStreams");
        f.setAccessible(true);
        Queue<?> opened = (Queue<?>) f.get(entry);

        boolean ok = true;
        for (int i = 0; i < 100; i++) {           // open/close cycles on a file that stays cached
            Tracking orig = new Tracking(false);
            CachedDataInputStream s = cache.open(remote, orig);
            s.read(); s.close(); s.close();
            ok &= orig.closed;
        }
        System.out.println("100 cycles: all originals closed=" + ok + "  openedStreams.size=" + opened.size());
        ok &= opened.isEmpty();

        Tracking bad = new Tracking(true);
        CachedDataInputStream s = cache.open(remote, bad);
        String thrown = "none";
        try { s.close(); } catch (IOException e) { thrown = e.getMessage(); }
        System.out.println("failing close: thrown=" + thrown + "  unregistered=" + opened.isEmpty() + "  isClosed=" + s.isClosed());
        ok &= opened.isEmpty() && "boom".equals(thrown);
        cache.close();
        System.out.println(ok ? "PASS" : "FAIL");
        System.exit(ok ? 0 : 1);
    }
}
