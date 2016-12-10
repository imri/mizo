package hbase;

import com.google.common.collect.AbstractIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by imrihecht on 12/10/16.
 */
public class MizoHFileIterator extends AbstractIterator<Cell> {
    private static final Logger log = LoggerFactory.getLogger(MizoHFileIterator.class);

    /**
     * Inner scanner for a single HFile
     */
    private final HFileScanner hfileScanner;

    public MizoHFileIterator(HFileScanner scanner) {
        this.hfileScanner = scanner;
    }

    @Override
    protected Cell computeNext() {
        try {
            if (this.hfileScanner.next()) {
                return this.hfileScanner.getKeyValue();
            } else {
                return endOfData();
            }
        } catch (Exception e) {
            log.warn("Failed to get next cell from HFile due to exception (scanner: {}, exception: {})",
                    this.hfileScanner, e);

//            throw new RuntimeException("Failed to get next cell from HFile due to exception", e);
            return endOfData();
        }
    }

    /**
     * Given a path of an HFile, creates an iterator that reads Cells stored in it.
     * If any exception is thrown upon creation, logs a warning and returns an empty iterator
     * @todo: change this implementation to rethrow the exception thrown upon creation
     */
    public static Iterator<Cell> createIterator(FileSystem fs, Path path) {
        try {
            return new MizoHFileIterator(createScanner(fs, path));
        } catch (Exception e) {
            log.warn("Failed to create cells iterator for HFile due to exception (file: {}, exception: {})",
                    path, e);

//            throw new RuntimeException(e);
            return Collections.emptyIterator();
        }
    }

    /**
     * Creates an inner HFileScanner object for a given HFile path
     */
    public static HFileScanner createScanner(FileSystem fs, Path path) throws IOException {
        Configuration config = fs.getConf();
        HFile.Reader reader = HFile.createReader(fs, path, getCacheConfig(config), config);

        HFileScanner scanner = reader.getScanner(false, false);
        scanner.seekTo();

        return scanner;
    }

    /**
     * Creates an inner cache-config object for HFile scanner
     */
    public static CacheConfig getCacheConfig(Configuration config) {
        config.setFloat("hfile.block.cache.size", 0);

        return new CacheConfig(config);
    }
}
