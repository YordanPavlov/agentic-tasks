import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.state.forst.fs.ByteBufferReadableFSDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ReadFailCheck {
    static class Failing extends FSDataInputStream {
        boolean closed;
        public void seek(long p) throws IOException { throw new IOException("read failed"); }
        public long getPos() { return 0; }
        public int read() throws IOException { throw new IOException("read failed"); }
        public void close() { closed = true; }
    }

    public static void main(String[] a) throws Exception {
        List<Failing> built = new ArrayList<>();
        ByteBufferReadableFSDataInputStream in = new ByteBufferReadableFSDataInputStream(
                () -> { Failing f = new Failing(); built.add(f); return f; }, 4, 100);
        String thrown = "none";
        try { in.readFully(10, ByteBuffer.allocate(8)); } catch (IOException e) { thrown = e.getMessage(); }
        Failing pooled = built.get(built.size() - 1);  // the stream readFully took for the read
        System.out.println("streams built=" + built.size() + "  thrown=" + thrown + "  failed stream closed=" + pooled.closed);
        boolean ok = built.size() == 2 && pooled.closed && "read failed".equals(thrown);
        System.out.println(ok ? "PASS" : "FAIL");
        System.exit(ok ? 0 : 1);
    }
}
