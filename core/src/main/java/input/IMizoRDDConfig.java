package input;

import models.MizoEdge;
import models.MizoVertex;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;

/**
 * Created by imrihecht on 12/9/16.
 */
public interface IMizoRDDConfig extends Serializable {
    String regionDirectoriesPath();
    String titanConfigPath();
    String logConfigPath();

    Function<MizoVertex, Boolean> parseVertexProperties();
    Function2<MizoVertex, String, Boolean> parseVertexProperty();
    Function<MizoVertex, Boolean> parseInEdges();
    Function2<MizoVertex, String, Boolean> parseInEdge();
    Function<MizoVertex, Boolean> parseOutEdges();
    Function2<MizoVertex, String, Boolean> parseOutEdge();
    Function<MizoEdge, Boolean> parseEdgeProperties();
    Function2<MizoEdge, String, Boolean> parseEdgeProperty();
    Function<MizoEdge, Boolean> filterEdge();
}
