package models;

import com.google.common.collect.Maps;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by imriqwe on 26/08/2016.
 */
public class MizoEdge implements Serializable {
    private final String label;
    private final long relationId;
    private final MizoVertex vertex;
    private final boolean isOutEdge;
    private final long otherVertexId;
    private Map<String, String> properties;
    private final long typeId;

    public MizoEdge(String label, long typeId, long relationId, boolean isOutEdge,
                    MizoVertex vertex, long otherVertexId) {

        this.label = label;
        this.typeId = typeId;
        this.relationId = relationId;
        this.isOutEdge = isOutEdge;
        this.vertex = vertex;
        this.otherVertexId = otherVertexId;
    }

    public String label() {
        return label;
    }

    public long relationId() {
        return relationId;
    }

    public MizoVertex vertex() {
        return vertex;
    }

    public boolean isOutEdge() {
        return isOutEdge;
    }

    public long getTypeId() {
        return typeId;
    }

    public long otherVertexId() {
        return otherVertexId;
    }

    public Map<String, String> properties() {
        return properties;
    }

    public Map<String, String> initializedProperties() {
        if (properties == null) {
            properties = Maps.newHashMap();
        }

        return properties;
    }

    /**
     * Creates a Titan Edge Id of this edge
     */
    public String titanId() {
        return RelationIdentifier.get(new long[]{
                relationId,
                isOutEdge ? vertex.id() : otherVertexId,
                typeId,
                isOutEdge ? otherVertexId : vertex.id()}).toString();
    }
}
