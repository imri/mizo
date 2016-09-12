package models;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Created by imriqwe on 26/08/2016.
 */
public class MizoVertex {
    private long id;
    private Map<String, String> properties;
    private List<MizoEdge> inEdges;
    private List<MizoEdge> outEdges;

    public MizoVertex(long id) {

        this.id = id;
    }

    public Map<String, String> initializedProperties() {
        if (properties == null) {
            properties = Maps.newHashMap();
        }

        return properties;
    }

    public List<MizoEdge> initializedInEdges() {
        if (inEdges == null) {
            inEdges = Lists.newArrayList();
        }

        return inEdges;
    }

    public long id() {
        return id;
    }

    public Map<String, String> properties() {
        return properties;
    }

    public List<MizoEdge> inEdges() {
        return inEdges;
    }

    public List<MizoEdge> outEdges() {
        return outEdges;
    }


    public List<MizoEdge> initializedOutEdges() {
        if (outEdges == null) {
            outEdges = Lists.newArrayList();
        }

        return outEdges;
    }


}
