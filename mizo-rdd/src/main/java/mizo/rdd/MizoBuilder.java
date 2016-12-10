package mizo.rdd;

import mizo.core.IMizoRDDConfig;
import mizo.core.IMizoRelationParser;
import mizo.core.MizoEdge;
import mizo.core.MizoVertex;
import mizo.iterators.MizoEdgesIterator;
import mizo.iterators.MizoVerticesIterator;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassManifestFactory;

import java.util.Iterator;

/**
 * Created by imrihecht on 12/9/16.
 */
public class MizoBuilder implements IMizoRDDConfig {
    private static final Logger log = LoggerFactory.getLogger(MizoBuilder.class);

    private String regionDirectoriesPath;
    private String titanConfigPath;
    private String logConfigPath;

    private Function<MizoEdge, Boolean> filterEdge = e -> true;
    private Function<MizoVertex, Boolean> parseVertexProperties = v -> true;
    private Function2<MizoVertex, String, Boolean> parseVertexProperty = (v, property) -> true;
    private Function<MizoVertex, Boolean> parseInEdges = v -> true;
    private Function2<MizoVertex, String, Boolean> parseInEdge = (v, property) -> parseInEdges.call(v);
    private Function<MizoVertex, Boolean> parseOutEdges = v -> true;
    private Function2<MizoVertex, String, Boolean> parseOutEdge = (v, property) -> parseOutEdges.call(v);
    private Function<MizoEdge, Boolean> parseEdgeProperties = e -> true;
    private Function2<MizoEdge, String, Boolean> parseEdgeProperty = (e, property) -> true;

    public MizoBuilder(String mizoConfigPath) throws ConfigurationException {
        PropertiesConfiguration mizoConfig = new PropertiesConfiguration(mizoConfigPath);

        regionDirectoriesPath(mizoConfig.getString("region-directories-path"));
        titanConfigPath(mizoConfig.getString("titan-config-path"));
        logConfigPath(mizoConfig.getString("log-config-path"));
    }

    public MizoBuilder() {

    }

    public MizoRDD<MizoEdge> edgesRDD(SparkContext sc) {
        return new MizoRDD<MizoEdge>(sc, this, ClassManifestFactory.classType(MizoEdge.class)) {
            @Override
            public scala.collection.Iterator<MizoEdge> createRegionIterator(Iterator<IMizoRelationParser> relationsIterator) {
                return new MizoEdgesIterator(relationsIterator, this.config);
            }
        };
    }

    public MizoRDD<MizoVertex> verticesRDD(SparkContext sc) {
        return new MizoRDD<MizoVertex>(sc, this, ClassManifestFactory.classType(MizoVertex.class)) {
            @Override
            public scala.collection.Iterator<MizoVertex> createRegionIterator(Iterator<IMizoRelationParser> relationsIterator) {
                return new MizoVerticesIterator(relationsIterator, this.config);
            }
        };
    }


    /*
     * Setters
     */

    public MizoBuilder regionDirectoriesPath(String path) {
        this.regionDirectoriesPath = path;

        return this;
    }

    public MizoBuilder titanConfigPath(String path) {
        this.titanConfigPath = path;

        return this;
    }

    public MizoBuilder logConfigPath(String path) {
        this.logConfigPath = path;

        return this;
    }

    public MizoBuilder filterEdge(Function<MizoEdge, Boolean> predicate) {
        this.filterEdge = predicate;

        return this;
    }

    public MizoBuilder parseVertexProperties(Function<MizoVertex, Boolean> predicate) {
        this.parseVertexProperties = predicate;

        return this;
    }

    public MizoBuilder parseVertexProperty(Function2<MizoVertex, String, Boolean> predicate) {
        this.parseVertexProperty = predicate;

        return this;
    }

    public MizoBuilder parseEdgeProperties(Function<MizoEdge, Boolean> predicate) {
        this.parseEdgeProperties = predicate;

        return this;
    }

    public MizoBuilder parseEdgeProperty(Function2<MizoEdge, String, Boolean> predicate) {
        this.parseEdgeProperty = predicate;

        return this;
    }

    public MizoBuilder parseInEdges(Function<MizoVertex, Boolean> predicate) {
        this.parseInEdges = predicate;

        return this;
    }

    public MizoBuilder parseInEdge(Function2<MizoVertex, String, Boolean> predicate) {
        this.parseInEdge = predicate;

        return this;
    }

    public MizoBuilder parseOutEdges(Function<MizoVertex, Boolean> predicate) {
        this.parseOutEdges = predicate;

        return this;
    }

    public MizoBuilder parseOutEdge(Function2<MizoVertex, String, Boolean> predicate) {
        this.parseOutEdge = predicate;

        return this;
    }

    /**
     * Getters
     */

    @Override
    public String regionDirectoriesPath() {
        return regionDirectoriesPath;
    }

    @Override
    public String titanConfigPath() {
        return titanConfigPath;
    }

    @Override
    public String logConfigPath() {
        return logConfigPath;
    }

    @Override
    public Function<MizoVertex, Boolean> parseVertexProperties() {
        return parseVertexProperties;
    }

    @Override
    public Function2<MizoVertex, String, Boolean> parseVertexProperty() {
        return parseVertexProperty;
    }

    @Override
    public Function<MizoVertex, Boolean> parseInEdges() {
        return parseInEdges;
    }

    @Override
    public Function2<MizoVertex, String, Boolean> parseInEdge() {
        return parseInEdge;
    }

    @Override
    public Function<MizoVertex, Boolean> parseOutEdges() {
        return parseOutEdges;
    }

    @Override
    public Function2<MizoVertex, String, Boolean> parseOutEdge() {
        return parseOutEdge;
    }

    @Override
    public Function<MizoEdge, Boolean> parseEdgeProperties() {
        return parseEdgeProperties;
    }

    @Override
    public Function2<MizoEdge, String, Boolean> parseEdgeProperty() {
        return parseEdgeProperty;
    }

    @Override
    public Function<MizoEdge, Boolean> filterEdge() {
        return filterEdge;
    }

    public String version() {
        return "Mizo3.1.4";
    }
}
