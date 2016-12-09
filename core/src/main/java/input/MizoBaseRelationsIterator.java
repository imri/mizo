package input;


import models.MizoEdge;
import models.MizoVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.AbstractIterator;

import java.util.Iterator;

/**
 * Created by imrihecht on 12/9/16.
 */
public abstract class MizoBaseRelationsIterator<TReturn> extends AbstractIterator<TReturn> implements Iterator<TReturn> {
    private static final Logger log = LoggerFactory.getLogger(MizoBaseRelationsIterator.class);

    /**
     * Configuration and tunings for this iterator
     */
    protected IMizoRDDConfig config;

    /**
     * Counts how many vertices were reached so far
     */
    protected long verticesCounter = 0;

    public MizoBaseRelationsIterator(IMizoRDDConfig config) {
        this.config = config;
    }

    /**
     * Parses a given input using a Parser object --
     * If the parsed object is a system/unknown type, returns null.
     * If it is a property, calls handleProperty() and returns null.
     * If it is an edge, returns what returned from handleEdge().
     * @param relation Parser of a single relation
     * @param vertexToUpdate Vertex we are currently parseing its edges/properties
     * @return Null of instance of an edge
     */
    public MizoEdge getEdgeOrNull(IMizoRelationParser relation, MizoVertex vertexToUpdate) {
        if (!relation.isSystemType() && relation.isKnownType()) {
            if (relation.isProperty()) {
                handleProperty(relation, vertexToUpdate);
            } else if (relation.isOutEdge()) {
                return handleEdge(relation, vertexToUpdate, true);
            } else if (relation.isInEdge()) {
                return handleEdge(relation, vertexToUpdate, false);
            }
        }

        return null;
    }

    protected MizoVertex setupNewVertex(IMizoRelationParser relation) {
        if (++verticesCounter % 50000 == 0) {
            log.info("VERTICES counter: " + verticesCounter);
        }

        return new MizoVertex(relation.getVertexId());
    }

    protected MizoEdge handleEdge(IMizoRelationParser relation, MizoVertex vertexToUpdate, boolean isOutEdge) {
        if (shouldParseEdge(vertexToUpdate, relation.getTypeName(), isOutEdge)) {
            MizoEdge newEdge = new MizoEdge(relation.getTypeName(), relation.getTypeId(),
                    relation.getRelationId(), isOutEdge, vertexToUpdate, relation.getOtherVertexId());

            // the order of the following is critical!
            // if should not parse properties - do not check valueHasRemaining(),
            // it'll cause buffer instantiation for nothing!
            if (shouldParseEdgeProperties(newEdge) && relation.valueHasRemaining()) {
                parseEdgeProperties(relation, newEdge);
            }

            if (shouldFilterEdge(newEdge))  {
                return newEdge;
            }
        }

        return null;
    }

    protected void handleProperty(IMizoRelationParser relation, MizoVertex vertexToUpdate) {
        if (shouldParseVertexProperty(vertexToUpdate, relation.getTypeName())) {
            vertexToUpdate.initializedProperties().put(relation.getTypeName(), relation.readPropertyValue());
        }
    }

    protected void parseEdgeProperties(IMizoRelationParser relation, MizoEdge edgeToUpdate) {
        while (relation.valueHasRemaining()) {
            String edgePropertyName = relation.readPropertyName();

            if (edgePropertyName != null && shouldParseEdgeProperty(edgeToUpdate, edgePropertyName)) {
                edgeToUpdate.initializedProperties().put(edgePropertyName, relation.readPropertyValue());
            } else {
                relation.skipPropertyValue();
            }
        }
    }

    protected boolean shouldParseVertexProperty(MizoVertex vertex, String propertyName) {
        try {
            return this.config.parseVertexProperties().call(vertex) &&
                    this.config.parseVertexProperty().call(vertex, propertyName);
        } catch (Exception e) {
            log.warn("Vertex predicate invocation failed due to exception (vertex-id: {}, property-name: {}, exception: {})", vertex.id(), propertyName, e);
        }

        return true;
    }

    protected boolean shouldParseEdge(MizoVertex vertex, String edgeLabel, boolean isOutEdge) {
        try {
            return (isOutEdge ? this.config.parseOutEdges() : this.config.parseInEdges()).call(vertex) &&
                    (isOutEdge ? this.config.parseOutEdge() : this.config.parseInEdge()).call(vertex, edgeLabel);
        } catch (Exception e) {
            log.warn("Vertex predicate invocation failed due to exception (vertex-id: {}, out-edge: {}, edge-label: {}, exception: {})", vertex.id(), isOutEdge, edgeLabel, e);
        }

        return true;
    }

    protected boolean shouldParseEdgeProperties(MizoEdge edge) {
        try {
            return this.config.parseEdgeProperties().call(edge);
        } catch (Exception e) {
            log.warn("Vertex predicate invocation failed due to exception (vertex-id: {}, relation-id: {}, edge-label: {}, exception: {})", edge.vertex().id(), edge.relationId(), edge.label(), e);
        }

        return true;
    }

    protected boolean shouldParseEdgeProperty(MizoEdge edge, String propertyName) {
        try {
            return this.config.parseEdgeProperty().call(edge, propertyName);
        } catch (Exception e) {
            log.warn("Vertex predicate invocation failed due to exception (vertex-id: {}, property-name: {}, relation-id: {}, edge-label: {}, exception: {})", edge.vertex().id(), propertyName, edge.relationId(), edge.label(), e);
        }

        return true;
    }

    protected boolean shouldFilterEdge(MizoEdge edge) {
        try {
            return this.config.filterEdge().call(edge);
        } catch (Exception e) {
            log.warn("Vertex predicate invocation failed due to exception (vertex-id: {}, relation-id: {}, edge-label: {}, exception: {})", edge.vertex().id(), edge.relationId(), edge.label(), e);
        }

        return true;
    }
}
