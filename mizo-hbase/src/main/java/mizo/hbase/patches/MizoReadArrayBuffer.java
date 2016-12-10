package mizo.hbase.patches;

import com.thinkaurelius.titan.diskstorage.util.ReadArrayBuffer;

/**
 * Created by imrihecht on 12/10/16.
 */
public class MizoReadArrayBuffer extends ReadArrayBuffer {
    public MizoReadArrayBuffer(byte[] array, int offset, int limit) {
        super(array, offset, limit);
    }
}
