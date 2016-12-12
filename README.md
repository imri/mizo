# Mizo
### Super-fast Spark RDD for Titan Graph Database on HBase

Mizo enables you to perform Spark transformations and actions over a Titan DB, under the following circumstances:
  - It runs with an HBase backend
  - Its HBase internal data files (HFiles) are accessible via the network

Mizo was originally developed due to a lack of an efficient and quick OLAP engine on top of Titan.
OLAP over Titan was meant to be solved by libraries such as Faunus and Tinkerpop's SparkGraphComputer, but neither of the solutions can be used in production - the former is buggy and misses data, and the latter is generally a non-efficient mechanism that spills lots of data.
Moreover, both of the solutions rely on HBase API to retrieve the Graph data in bulk, but this API is by itself very slow. Mizo relies on HBase internal data files (called HFiles), parses them and builds vertices and edges from them - without interacting with HBase API.

### In production

Mizo was tested in production on a Titan Graph with a about ten billion vertices and hundreds of billions of edges.
Using a Spark cluster with total of 100 cores, it takes about 8 hours for Mizo to iterate over a graph with 2000 HBase regions.

### Limitations

Mizo is limited in terms of traversing the graph - it is intended for single-hop queries only, meaning that you can reach a vertex and its edges, but you cannot jump to the other vertex, you can only get its ID.
For example, Mizo can be used for counting how many vertices exist that have a property called 'first_name', but Mizo cannot be used to count edges that connect two vertices with a property called 'first_name', because only one vertex is available at a time.

You can run Mizo on a working HBase cluster. The problem here is that HBase performs regularly performs compactions, which basically change and delete HFiles. While not locking the HFiles, Mizo can suffer from data misses if an HFile is removed (it skips the file and moves next). The best practice is to run Mizo on an idle HBase cluster.

### RDDs and customization

Mizo supports different levels of customization -- by default, it'll parse every vertex and edge. More accurately, due to Titan's internal data structure, which keeps each edge twice - one time on the 'in' vertex and another time on the 'out' vertex, Mizo will return each edge twice (on time when parsing the HBase region containing the 'in' vertex, and another time while parsing the 'out' vertex). You can prevent this by customizing Mizo to parse only in/out edges.

Mizo exposes two types of RDDs:
  - `MizoVerticesRDD` is an RDD of vertices, with their in and/or out edges and their properties. It is much more heavy in terms of memory, because each vertex returned also contains a collection of edges.
  - `MizoEdgesRDD` is an RDD of edges, with the vertex it originated from. An edge is much more lighter in terms of memory usage - each edge contains a vertex inside, but that vertex does not contain a list of vertices (more accurately, the list is always null), so there are no heavy lists to keep in memory.

### Getting started

Using Mizo for counting edges on graph:

```
import mizo.rdd.MizoBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class MizoEdgesCounter {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Mizo Edges Counter")
                .setMaster("local[1]")
                .set("spark.executor.memory", "4g")
                .set("spark.executor.cores", "1")
                .set("spark.rpc.askTimeout", "1000000")
                .set("spark.rpc.frameSize", "1000000")
                .set("spark.network.timeout", "1000000")
                .set("spark.rdd.compress", "true")
                .set("spark.core.connection.ack.wait.timeout", "6000")
                .set("spark.driver.maxResultSize", "100m")
                .set("spark.task.maxFailures", "20")
                .set("spark.shuffle.io.maxRetries", "20");

        SparkContext sc = new SparkContext(conf);

        long count = new MizoBuilder()
                .titanConfigPath("titan-graph.properties")
                .regionDirectoriesPath("hdfs://my-graph/*/e")
                .parseInEdges(v -> false)
                .edgesRDD(sc)
                .toJavaRDD()
                .count();

        System.out.println("Edges count is: " + count);
    }
}
```


Using Mizo for counting vertices on graph:

```
import mizo.rdd.MizoBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class MizoVerticesCounter {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Mizo Edges Counter")
                .setMaster("local[1]")
                .set("spark.executor.memory", "4g")
                .set("spark.executor.cores", "1")
                .set("spark.rpc.askTimeout", "1000000")
                .set("spark.rpc.frameSize", "1000000")
                .set("spark.network.timeout", "1000000")
                .set("spark.rdd.compress", "true")
                .set("spark.core.connection.ack.wait.timeout", "6000")
                .set("spark.driver.maxResultSize", "100m")
                .set("spark.task.maxFailures", "20")
                .set("spark.shuffle.io.maxRetries", "20");

        SparkContext sc = new SparkContext(conf);

        long count = new MizoBuilder()
                .titanConfigPath("titan-graph.properties")
                .regionDirectoriesPath("hdfs://my-graph/*/e")
                .parseInEdges(v -> false)
                .verticesRDD(sc)
                .toJavaRDD()
                .count();

        System.out.println("Vertices count is: " + count);
    }
}
```