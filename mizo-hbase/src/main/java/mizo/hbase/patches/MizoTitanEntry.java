package mizo.hbase.patches;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryMetaData;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;

import java.util.Map;

/**
 * Created by imrihecht on 12/23/16.
 */
public class MizoTitanEntry extends MizoTitanStaticBuffer implements Entry {
    public MizoTitanEntry(byte[] array, int offsetKey, int limitKey, int offsetValue, int limitValue) {
        super(array, offsetKey, limitKey, offsetValue, limitValue);
    }

    public int getValuePosition() {
        return super.valuePosition();
    }

    public boolean hasValue() {
        return super.valuePosition() < this.length();
    }

    public StaticBuffer getColumn() {
        throw new RuntimeException("Not implemented");
    }

    public <T> T getColumnAs(Factory<T> factory) {
        throw new RuntimeException("Not implemented");
    }

    public StaticBuffer getValue() {

        throw new RuntimeException("Not implemented");
    }

    public <T> T getValueAs(Factory<T> factory) {
        throw new RuntimeException("Not implemented");
    }

    public RelationCache getCache() {

        throw new RuntimeException("Not implemented");
    }

    public void setCache(RelationCache cache) {
        throw new RuntimeException("Not implemented");
    }

    public boolean hasMetaData() {
        return false;
    }

    public Map<EntryMetaData, Object> getMetaData() {
        return EntryMetaData.EMPTY_METADATA;
    }
}
