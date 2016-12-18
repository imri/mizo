package mizo.iterators;

import mizo.core.IMizoRDDConfig;
import mizo.core.IMizoRelationParser;
import mizo.core.MizoEdge;
import mizo.core.MizoVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Created by imrihecht on 12/10/16.
 */
public class MizoEdgesIterator extends MizoBaseRelationsIterator<MizoEdge> {
    private static final Logger log = LoggerFactory.getLogger(MizoEdgesIterator.class);

    /**
     * Next edge to be returned when calling next()
     */
    protected MizoEdge edgeToReturn;

    /**
     * Each call to this iterator returns a relation (vertex property or in/out edge) to be parsed
     */
    protected Iterator<IMizoRelationParser> relationsIterator;

    /**
     * The current vertex we are parsing its relations
     */
    private MizoVertex currentVertex = null;

    /**
     * Last vertex ID we reached
     */
    private long lastVertexId = 0;

    public MizoEdgesIterator(Iterator<IMizoRelationParser> relationsIterator, IMizoRDDConfig config) {
        super(config);

        this.relationsIterator = relationsIterator;
    }

    @Override
    public boolean hasNext() {
        if (edgeToReturn != null) {
            return true;
        }

        edgeToReturn = null;

        while (edgeToReturn == null && this.relationsIterator.hasNext()) {
            IMizoRelationParser relation = this.relationsIterator.next();

            if (lastVertexId != relation.getVertexId()) {
                currentVertex = setupNewVertex(relation);
                lastVertexId = relation.getVertexId();
            }

            edgeToReturn = getEdgeOrNull(relation, currentVertex);
        }

        return edgeToReturn != null;
    }

    @Override
    public MizoEdge next() {
        MizoEdge toReturn = edgeToReturn;
        edgeToReturn = null;

        return toReturn;
    }
}
