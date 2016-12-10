import com.google.common.base.Predicate;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.Order;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import org.apache.tinkerpop.gremlin.structure.*;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Created by imrihecht on 12/10/16.
 */
public class MizoTitanRelationType implements Serializable, InternalRelationType {

    private final SchemaStatus status;
    private final long[] sortKey;
    private final String name;
    private final long id;
    private final boolean isInvisibleType;
    private final long[] signature;
    private final Order sortOrder;
    private final Multiplicity multiplicity;
    private final ConsistencyModifier consistencyModifier;
    private final Integer ttl;
    private final boolean isPropertyKey;
    private final boolean isEdgeLabel;

    public MizoTitanRelationType(InternalRelationType relationType) {
        status = relationType.getStatus();
        sortKey = relationType.getSortKey();
        name = relationType.name();
        id = relationType.longId();
        isInvisibleType = relationType.isInvisibleType();
        signature = relationType.getSignature();
        sortOrder = relationType.getSortOrder();
        multiplicity = relationType.multiplicity();
        consistencyModifier = relationType.getConsistencyModifier();
        ttl = relationType.getTTL();
        isPropertyKey = relationType.isPropertyKey();
        isEdgeLabel = relationType.isEdgeLabel();
    }

    @Override
    public boolean isInvisibleType() {
        return isInvisibleType;
    }

    @Override
    public long[] getSignature() {
        return signature;
    }

    @Override
    public long[] getSortKey() {
        return sortKey;
    }

    @Override
    public Order getSortOrder() {
        return sortOrder;
    }

    @Override
    public Multiplicity multiplicity() {
        return multiplicity;
    }

    @Override
    public ConsistencyModifier getConsistencyModifier() {
        return consistencyModifier;
    }

    @Override
    public Integer getTTL() {
        return ttl;
    }

    @Override
    public boolean isUnidirected(Direction direction) {
        return false;
    }

    @Override
    public InternalRelationType getBaseType() {
        return null;
    }

    @Override
    public Iterable<InternalRelationType> getRelationIndexes() {
        return null;
    }

    @Override
    public SchemaStatus getStatus() {
        return status;
    }

    @Override
    public Iterable<IndexType> getKeyIndexes() {
        return null;
    }

    @Override
    public boolean isPropertyKey() {
        return isPropertyKey;
    }

    @Override
    public boolean isEdgeLabel() {
        return isEdgeLabel;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public InternalVertex it() {
        return null;
    }

    @Override
    public StandardTitanTx tx() {
        return null;
    }

    @Override
    public void setId(long l) {

    }

    @Override
    public byte getLifeCycle() {
        return 0;
    }

    @Override
    public boolean isInvisible() {
        return false;
    }

    @Override
    public void removeRelation(InternalRelation internalRelation) {

    }

    @Override
    public boolean addRelation(InternalRelation internalRelation) {
        return false;
    }

    @Override
    public List<InternalRelation> getAddedRelations(Predicate<InternalRelation> predicate) {
        return null;
    }

    @Override
    public EntryList loadRelations(SliceQuery sliceQuery, Retriever<SliceQuery, EntryList> retriever) {
        return null;
    }

    @Override
    public boolean hasLoadedRelations(SliceQuery sliceQuery) {
        return false;
    }

    @Override
    public boolean hasRemovedRelations() {
        return false;
    }

    @Override
    public boolean hasAddedRelations() {
        return false;
    }

    @Override
    public TitanEdge addEdge(String s, Vertex vertex, Object... objects) {
        return null;
    }

    @Override
    public <V> TitanVertexProperty<V> property(String s, V v, Object... objects) {
        return null;
    }

    @Override
    public <V> TitanVertexProperty<V> property(VertexProperty.Cardinality cardinality, String s, V v, Object... objects) {
        return null;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... strings) {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... strings) {
        return null;
    }

    @Override
    public VertexLabel vertexLabel() {
        return null;
    }

    @Override
    public VertexCentricQueryBuilder query() {
        return null;
    }

    @Override
    public boolean isModified() {
        return false;
    }

    @Override
    public long longId() {
        return id;
    }

    @Override
    public boolean hasId() {
        return false;
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... strings) {
        return null;
    }

    @Override
    public <V> V valueOrNull(PropertyKey propertyKey) {
        return null;
    }

    @Override
    public boolean isNew() {
        return false;
    }

    @Override
    public boolean isLoaded() {
        return false;
    }

    @Override
    public boolean isRemoved() {
        return false;
    }
}
