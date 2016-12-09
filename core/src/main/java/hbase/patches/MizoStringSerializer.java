package hbase.patches;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Namifiable;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.OrderPreservingSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.SupportsNullSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.CharacterSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.StringSerializer;
import com.thinkaurelius.titan.util.encoding.StringEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by imrihecht on 12/10/16.
 */
public class MizoStringSerializer {
    public static final int MAX_NUM_COMPRESSORS = 8;
    public static final long COMPRESSOR_BIT_MASK = 7L;
    public static final long COMPRESSOR_BIT_LEN = 3L;

    public static enum CompressionType {
        NO_COMPRESSION {
            public byte[] compress(String text) {
                throw new UnsupportedOperationException();
            }

            public String decompress(ScanBuffer buffer, int numBytes) {
                throw new UnsupportedOperationException();
            }
        },
        GZIP {
            public byte[] compress(String text) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();

                try {
                    GZIPOutputStream e = new GZIPOutputStream(baos);
                    e.write(text.getBytes("UTF-8"));
                    e.close();
                } catch (IOException var4) {
                    throw new RuntimeException(var4);
                }

                return baos.toByteArray();
            }

            public String decompress(final ScanBuffer buffer, final int numBytes) {
                try {
                    GZIPInputStream e = new GZIPInputStream(new InputStream() {
                        int bytesRead = 0;

                        public int read() throws IOException {
                            return ++this.bytesRead > numBytes?-1:255 & buffer.getByte();
                        }
                    });
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    byte[] bytes = new byte[8192];

                    int len;
                    while((len = e.read(bytes)) > 0) {
                        baos.write(bytes, 0, len);
                    }

                    return new String(baos.toByteArray(), "UTF-8");
                } catch (IOException var7) {
                    throw new RuntimeException(var7);
                }
            }
        };

        private CompressionType() {
        }

        public abstract byte[] compress(String var1);

        public abstract String decompress(ScanBuffer var1, int var2);

        public int getId() {
            return this.ordinal();
        }

        public static CompressionType getFromId(int id) {
            CompressionType[] var1 = values();
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
                CompressionType ct = var1[var3];
                if(ct.getId() == id) {
                    return ct;
                }
            }

            throw new IllegalArgumentException("Unknown compressor type for id: " + id);
        }
    }
}
