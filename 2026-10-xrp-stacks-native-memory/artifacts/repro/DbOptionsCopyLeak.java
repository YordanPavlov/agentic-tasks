import org.forstdb.BlockBasedTableConfig;
import org.forstdb.ColumnFamilyDescriptor;
import org.forstdb.ColumnFamilyHandle;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.CompressionType;
import org.forstdb.DBOptions;
import org.forstdb.FlushOptions;
import org.forstdb.LRUCache;
import org.forstdb.ReadOptions;
import org.forstdb.RocksDB;
import org.forstdb.WriteBufferManager;
import org.forstdb.WriteOptions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Does an unclosed native DBOptions copy keep ForSt's shared block cache alive after
 * everything else is closed? Mirrors ForSt's SLOT_SHARED_MANAGED setup: one LRUCache, a
 * WriteBufferManager charged to it, DBOptions carrying the WBM, and the copy that
 * ForStIncrementalRestoreOperation.restoreTempDBInstance makes and never closes.
 *
 * usage: DbOptionsCopyLeak none|leak|closed   (run with jemalloc, dirty/muzzy decay 0)
 */
public class DbOptionsCopyLeak {
    static final List<Object> KEEP = new ArrayList<>(); // keeps the leaked copy's Java object alive

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        RocksDB.loadLibrary();
        long base = rssMiB();

        LRUCache cache = new LRUCache(1L << 30);
        WriteBufferManager wbm = new WriteBufferManager(64L << 20, cache);
        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
                .setWriteBufferManager(wbm);
        DBOptions copy = mode.equals("none") ? null : new DBOptions(dbOptions);
        BlockBasedTableConfig table = new BlockBasedTableConfig()
                .setBlockCache(cache).setCacheIndexAndFilterBlocks(true);
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                .setTableFormatConfig(table).setCompressionType(CompressionType.NO_COMPRESSION);

        Path dir = Files.createTempDirectory("forst-leak-");
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        RocksDB db = RocksDB.open(dbOptions, dir.toString(),
                List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions)), handles);

        int n = 300_000;
        byte[] value = new byte[1000];
        try (WriteOptions wo = new WriteOptions().setDisableWAL(true)) {
            for (int i = 0; i < n; i++) {
                value[0] = (byte) i;
                db.put(wo, key(i), value);
            }
        }
        try (FlushOptions fo = new FlushOptions().setWaitForFlush(true)) {
            db.flush(fo);
        }
        try (ReadOptions ro = new ReadOptions()) {
            for (int i = 0; i < n; i++) db.get(ro, key(i));
        }
        long filled = rssMiB();
        long cacheUsage = cache.getUsage() >> 20;

        handles.forEach(ColumnFamilyHandle::close);
        db.close();
        cfOptions.close();
        dbOptions.close();
        wbm.close();
        cache.close();
        if (mode.equals("closed")) copy.close();
        if (mode.equals("leak")) KEEP.add(copy);
        System.gc();
        Thread.sleep(2000);
        long after = rssMiB();

        System.out.printf("mode=%-6s base=%d MiB filled=%d MiB (cache %d MiB) after-close=%d MiB retained=%d MiB%n",
                mode, base, filled, cacheUsage, after, after - base);
    }

    static byte[] key(int i) {
        return String.format("key-%09d", i).getBytes();
    }

    static long rssMiB() throws Exception {
        for (String l : Files.readAllLines(Path.of("/proc/self/status")))
            if (l.startsWith("VmRSS:")) return Long.parseLong(l.replaceAll("\\D", "")) / 1024;
        return -1;
    }
}
