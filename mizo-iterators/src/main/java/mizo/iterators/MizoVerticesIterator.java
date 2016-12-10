package mizo.iterators;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
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
public class MizoVerticesIterator extends  MizoBaseRelationsIterator<MizoVertex> {
    private static final Logger log = LoggerFactory.getLogger(MizoVerticesIterator.class);

    /**
     * Each call to this iterator returns a relation (vertex property or in/out edge) to be parsed
     */
    protected PeekingIterator<IMizoRelationParser> relationsIterator;

    public MizoVerticesIterator(Iterator<IMizoRelationParser> relationsIterator, IMizoRDDConfig config) {
        super(config);

        this.relationsIterator = Iterators.peekingIterator(relationsIterator);
    }

    @Override
    public boolean hasNext() {
        return relationsIterator.hasNext();
    }

    @Override
    public MizoVertex next() {
        long lastVertexId = this.relationsIterator.peek().getVertexId();
        MizoVertex currentVertex = setupNewVertex(this.relationsIterator.peek());

        while (this.relationsIterator.hasNext() && lastVertexId == this.relationsIterator.peek().getVertexId()) {
            MizoEdge edgeOrNull = getEdgeOrNull(this.relationsIterator.next(), currentVertex);

            if (edgeOrNull != null) {
                if (edgeOrNull.isOutEdge()) {
                    currentVertex.initializedOutEdges().add(edgeOrNull);
                } else {
                    currentVertex.initializedInEdges().add(edgeOrNull);
                }
            }
        }

        return currentVertex;
    }
}
