/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.gradoop.io.hbase.HBaseReader;
import org.gradoop.io.hbase.HBaseWriter;
import org.gradoop.io.json.JsonReader;
import org.gradoop.io.json.JsonWriter;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.EdgeDataFactory;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.GraphDataFactory;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.api.VertexDataFactory;
import org.gradoop.model.impl.pojo.DefaultEdgeData;
import org.gradoop.model.impl.pojo.DefaultEdgeDataFactory;
import org.gradoop.model.impl.pojo.DefaultGraphData;
import org.gradoop.model.impl.pojo.DefaultGraphDataFactory;
import org.gradoop.model.impl.pojo.DefaultVertexData;
import org.gradoop.model.impl.pojo.DefaultVertexDataFactory;
import org.gradoop.model.impl.tuples.Subgraph;
import org.gradoop.storage.api.EPGMStore;
import org.gradoop.storage.api.PersistentEdgeData;
import org.gradoop.storage.api.PersistentEdgeDataFactory;
import org.gradoop.storage.api.PersistentGraphData;
import org.gradoop.storage.api.PersistentGraphDataFactory;
import org.gradoop.storage.api.PersistentVertexData;
import org.gradoop.storage.api.PersistentVertexDataFactory;
import org.gradoop.util.FlinkConstants;

import java.util.Collection;

/**
 * Enables the access an EPGM instance.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class EPGMDatabase<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> {

  /**
   * Used to create new vertex data.
   */
  private final VertexDataFactory<VD> vertexDataFactory;

  /**
   * Used to create new edge data.
   */
  private final EdgeDataFactory<ED> edgeDataFactory;

  /**
   * Used to create new graph data.
   */
  private final GraphDataFactory<GD> graphDataFactory;

  /**
   * Graph data associated with that logical graph.
   */
  private final GD databaseData;

  /**
   * Database graph representing the vertex and edge space.
   */
  private GraphCollection<VD, ED, GD> database;

  /**
   * Flink execution environment.
   */
  private final ExecutionEnvironment env;

  /**
   * Creates a new EPGM database from the given arguments.
   *
   * @param vertices          vertex data set
   * @param edges             edge data set
   * @param graphHeads        graph data set
   * @param vertexDataFactory vertex data factory
   * @param edgeDataFactory   edge data factory
   * @param graphDataFactory  graph data factory
   * @param env               Flink execution environment
   */
  private EPGMDatabase(DataSet<VD> vertices,
    DataSet<ED> edges, DataSet<GD> graphHeads,
    VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory, GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {
    this.vertexDataFactory = vertexDataFactory;
    this.edgeDataFactory = edgeDataFactory;
    this.graphDataFactory = graphDataFactory;
    this.database =
      new GraphCollection<>(vertices, edges, graphHeads,
        vertexDataFactory, edgeDataFactory, graphDataFactory, env);
    this.env = env;
    this.databaseData = graphDataFactory
      .createGraphData(FlinkConstants.DATABASE_GRAPH_ID);
  }

  /**
   * Creates a database from JSON files. Paths can be local (file://) or HDFS
   * (hdfs://).
   * <p/>
   * Uses default factories for POJO creation.
   *
   * @param vertexFile path to vertex data file
   * @param edgeFile   path to edge data file
   * @param env        Flink execution environment
   * @return EPGM database with default factories.
   * @see DefaultVertexDataFactory
   * @see DefaultEdgeDataFactory
   * @see DefaultGraphDataFactory
   */
  @SuppressWarnings("unchecked")
  public static EPGMDatabase<DefaultVertexData, DefaultEdgeData,
    DefaultGraphData> fromJsonFile(
    String vertexFile, String edgeFile, ExecutionEnvironment env) {
    return fromJsonFile(vertexFile, edgeFile, new DefaultVertexDataFactory(),
      new DefaultEdgeDataFactory(), new DefaultGraphDataFactory(), env);
  }

  /**
   * Creates a database from JSON files. Paths can be local (file://) or HDFS
   * (hdfs://). Data POJOs are created using the given factories.
   *
   * @param vertexFile        path to vertex data file
   * @param edgeFile          path to edge data file
   * @param vertexDataFactory vertex data factory
   * @param edgeDataFactory   edge data factory
   * @param graphDataFactory  graph data factory
   * @param env               Flink execution environment
   * @param <VD>              vertex data type
   * @param <ED>              edge data type
   * @param <GD>              graph data type
   * @return EPGM database
   */
  public static <VD extends VertexData, ED extends EdgeData, GD extends
    GraphData> EPGMDatabase fromJsonFile(
    String vertexFile, String edgeFile, VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory, GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {
    return fromJsonFile(vertexFile, edgeFile, null, vertexDataFactory,
      edgeDataFactory, graphDataFactory, env);
  }

  /**
   * Creates a database from JSON files. Paths can be local (file://) or HDFS
   * (hdfs://).
   * <p/>
   * Uses default factories for POJO creation.
   *
   * @param vertexFile vertex data file
   * @param edgeFile   edge data file
   * @param graphFile  graph data file
   * @param env        Flink execution environment
   * @return EPGM database
   * @see DefaultVertexDataFactory
   * @see DefaultEdgeDataFactory
   * @see DefaultGraphDataFactory
   */
  @SuppressWarnings("unchecked")
  public static EPGMDatabase<DefaultVertexData, DefaultEdgeData,
    DefaultGraphData> fromJsonFile(
    String vertexFile, String edgeFile, String graphFile,
    ExecutionEnvironment env) {
    return fromJsonFile(vertexFile, edgeFile, graphFile,
      new DefaultVertexDataFactory(), new DefaultEdgeDataFactory(),
      new DefaultGraphDataFactory(), env);
  }

  /**
   * Creates a database from JSON files. Paths can be local (file://) or HDFS
   * (hdfs://).
   *
   * @param vertexFile        vertex data file
   * @param edgeFile          edge data file
   * @param graphFile         graph data file
   * @param vertexDataFactory vertex data factory
   * @param edgeDataFactory   edge data factory
   * @param graphDataFactory  graph data factory
   * @param env               Flink execution environment
   * @param <VD>              vertex data type
   * @param <ED>              edge data type
   * @param <GD>              graph data type
   * @return EPGM database
   */
  @SuppressWarnings("unchecked")
  public static <VD extends VertexData, ED extends EdgeData, GD extends
    GraphData> EPGMDatabase fromJsonFile(
    String vertexFile, String edgeFile, String graphFile,
    VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory, GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {

    // used for type hinting when loading vertex data
    TypeInformation<VD> vertexTypeInfo = (TypeInformation<VD>)
      TypeExtractor.createTypeInfo(vertexDataFactory.getType());
    // used for type hinting when loading edge data
    TypeInformation<ED> edgeTypeInfo = (TypeInformation<ED>)
      TypeExtractor.createTypeInfo(edgeDataFactory.getType());
    // used for type hinting when loading graph data
    TypeInformation<GD> graphTypeInfo = (TypeInformation<GD>)
      TypeExtractor.createTypeInfo(graphDataFactory.getType());

    // read vertex, edge and graph data
    DataSet<VD> vertices = env.readTextFile(vertexFile)
      .map(new JsonReader.JsonToVertexMapper<>(vertexDataFactory))
      .returns(vertexTypeInfo);
    DataSet<ED> edges = env.readTextFile(edgeFile)
      .map(new JsonReader.JsonToEdgeMapper<>(edgeDataFactory))
      .returns(edgeTypeInfo);
    DataSet<GD> graphHeads;
    if (graphFile != null) {
      graphHeads = env.readTextFile(graphFile)
        .map(new JsonReader.JsonToGraphMapper<>(graphDataFactory))
        .returns(graphTypeInfo);
    } else {
      graphHeads = env.fromCollection(Lists.newArrayList(
        graphDataFactory.createGraphData(FlinkConstants.DATABASE_GRAPH_ID)));
    }

    return new EPGMDatabase<>(vertices, edges, graphHeads, vertexDataFactory,
      edgeDataFactory, graphDataFactory, env);
  }

  /**
   * Writes the epgm database into three separate JSON files. {@code
   * vertexFile} contains all vertex data, {@code edgeFile} contains all edge
   * data and {@code graphFile} contains graph data of all logical graphs.
   * <p/>
   * Operation uses Flink to write the internal datasets, thus writing to
   * local file system ({@code file://}) as well as HDFS ({@code hdfs://}) is
   * supported.
   *
   * @param vertexFile vertex data output file
   * @param edgeFile   edge data output file
   * @param graphFile  graph data output file
   * @throws Exception
   */
  public void writeAsJson(final String vertexFile, final String edgeFile,
    final String graphFile) throws Exception {
    getDatabaseGraph().getVertices()
      .writeAsFormattedText(vertexFile,
        new JsonWriter.VertexTextFormatter<VD>());
    getDatabaseGraph().getEdges()
      .writeAsFormattedText(edgeFile, new JsonWriter.EdgeTextFormatter<ED>());
    getCollection().getGraphHeads()
      .writeAsFormattedText(graphFile, new JsonWriter.GraphTextFormatter<GD>());
    env.execute();
  }

  /**
   * Writes the EPGM database instance to HBase using the given arguments.
   * <p/>
   * HBase tables must be created before calling this method.
   *
   * @param epgmStore                   EPGM store to handle HBase
   * @param persistentVertexDataFactory persistent vertex data factory
   * @param persistentEdgeDataFactory   persistent edge data factory
   * @param persistentGraphDataFactory  persistent graph data factory
   * @param <PVD>                       persistent vertex data type
   * @param <PED>                       persistent edge data type
   * @param <PGD>                       persistent graph data type
   * @throws Exception
   */
  public <PVD extends PersistentVertexData<ED>, PED extends
    PersistentEdgeData<VD>, PGD extends PersistentGraphData> void writeToHBase(
    EPGMStore<VD, ED, GD> epgmStore,
    final PersistentVertexDataFactory<VD, ED, PVD> persistentVertexDataFactory,
    final PersistentEdgeDataFactory<ED, VD, PED> persistentEdgeDataFactory,
    final PersistentGraphDataFactory<GD, PGD> persistentGraphDataFactory) throws
    Exception {

    HBaseWriter<VD, ED, GD> hBaseWriter = new HBaseWriter<>();

    // transform graph data to persistent graph data and write it
    hBaseWriter.writeGraphHeads(this, epgmStore.getGraphDataHandler(),
      persistentGraphDataFactory, epgmStore.getGraphDataTableName());
    env.execute();

    // transform vertex data to persistent vertex data and write it
    hBaseWriter.writeVertices(this, epgmStore.getVertexDataHandler(),
      persistentVertexDataFactory, epgmStore.getVertexDataTableName());
    env.execute();

    // transform edge data to persistent edge data and write it
    hBaseWriter.writeEdges(this, epgmStore.getEdgeDataHandler(),
      persistentEdgeDataFactory, epgmStore.getEdgeDataTableName());
    env.execute();
  }

  /**
   * Creates a database from collections of vertex and edge data objects.
   * <p/>
   * Uses default factories for POJO creation.
   *
   * @param vertexDataCollection collection of vertex data objects
   * @param edgeDataCollection   collection of edge data objects
   * @param env                  Flink execution environment
   * @return EPGM database
   * @see DefaultVertexDataFactory
   * @see DefaultEdgeDataFactory
   * @see DefaultGraphDataFactory
   */
  @SuppressWarnings("unchecked")
  public static EPGMDatabase<DefaultVertexData, DefaultEdgeData,
    DefaultGraphData> fromCollection(
    Collection<DefaultVertexData> vertexDataCollection,
    Collection<DefaultEdgeData> edgeDataCollection, ExecutionEnvironment env) {
    return fromCollection(vertexDataCollection, edgeDataCollection, null,
      new DefaultVertexDataFactory(), new DefaultEdgeDataFactory(),
      new DefaultGraphDataFactory(), env);
  }

  /**
   * Creates a database from collections of vertex and data objects.
   *
   * @param vertexDataCollection collection of vertex data objects
   * @param edgeDataCollection   collection of edge data objects
   * @param vertexDataFactory    vertex data factory
   * @param edgeDataFactory      edge data factory
   * @param graphDataFactory     graph data factory
   * @param env                  Flink execution environment
   * @param <VD>                 vertex data type
   * @param <ED>                 edge data type
   * @param <GD>                 graph data typeFlink execution environment
   * @return EPGM database
   */
  public static <VD extends VertexData, ED extends EdgeData, GD extends
    GraphData> EPGMDatabase fromCollection(
    Collection<VD> vertexDataCollection, Collection<ED> edgeDataCollection,
    VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory, GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {
    return fromCollection(vertexDataCollection, edgeDataCollection, null,
      vertexDataFactory, edgeDataFactory, graphDataFactory, env);
  }

  /**
   * Creates a database from collections of vertex, edge and graph data
   * objects.
   * <p/>
   * Uses default factories for POJO creation.
   *
   * @param vertexDataCollection collection of vertex data objects
   * @param edgeDataCollection   collection of edge data objects
   * @param graphDataCollection  collection of graph data objects
   * @param env                  Flink execution environment
   * @return EPGM database
   * @see DefaultVertexDataFactory
   * @see DefaultEdgeDataFactory
   * @see DefaultGraphDataFactory
   */
  @SuppressWarnings("unchecked")
  public static EPGMDatabase<DefaultVertexData, DefaultEdgeData,
    DefaultGraphData> fromCollection(
    Collection<DefaultVertexData> vertexDataCollection,
    Collection<DefaultEdgeData> edgeDataCollection,
    Collection<DefaultGraphData> graphDataCollection,
    ExecutionEnvironment env) {
    return fromCollection(vertexDataCollection, edgeDataCollection,
      graphDataCollection, new DefaultVertexDataFactory(),
      new DefaultEdgeDataFactory(), new DefaultGraphDataFactory(), env);
  }

  /**
   * Creates a database from collections of vertex and data objects.
   *
   * @param vertexDataCollection collection of vertices
   * @param edgeDataCollection   collection of edges
   * @param graphDataCollection  collection of graph heads
   * @param vertexDataFactory    vertex data factory
   * @param edgeDataFactory      edge data factory
   * @param graphDataFactory     graph data factory
   * @param env                  Flink execution environment
   * @param <VD>                 vertex data type
   * @param <ED>                 edge data type
   * @param <GD>                 graph data type
   * @return EPGM database
   */
  @SuppressWarnings("unchecked")
  public static <VD extends VertexData, ED extends EdgeData, GD extends
    GraphData> EPGMDatabase fromCollection(
    Collection<VD> vertexDataCollection, Collection<ED> edgeDataCollection,
    Collection<GD> graphDataCollection, VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory, GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {
    DataSet<VD> vertices = env.fromCollection(vertexDataCollection);
    DataSet<ED> edges = env.fromCollection(edgeDataCollection);
    DataSet<GD> graphHeads;
    if (graphDataCollection != null) {
      graphHeads = env.fromCollection(graphDataCollection);
    } else {
      graphHeads = env.fromCollection(Lists.newArrayList(
        graphDataFactory.createGraphData(FlinkConstants.DATABASE_GRAPH_ID)));
    }

    return new EPGMDatabase<>(vertices, edges, graphHeads,
      vertexDataFactory, edgeDataFactory, graphDataFactory, env);
  }

  /**
   * Creates an EPGM database from an EPGM Store using the given arguments.
   *
   * @param epgmStore EPGM store
   * @param env       Flink execution environment
   * @param <VD>      vertex data type
   * @param <ED>      edge data type
   * @param <GD>      graph data type
   * @return EPGM database
   */
  @SuppressWarnings("unchecked")
  public static <VD extends VertexData, ED extends EdgeData, GD extends
    GraphData> EPGMDatabase<VD, ED, GD> fromHBase(
    EPGMStore<VD, ED, GD> epgmStore, final ExecutionEnvironment env) {

    // used for type hinting when loading vertex data
    TypeInformation<Vertex<Long, VD>> vertexTypeInfo =
      new TupleTypeInfo(Vertex.class, BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.createTypeInfo(
          epgmStore.getVertexDataHandler().getVertexDataFactory().getType()));
    // used for type hinting when loading edge data
    TypeInformation<Edge<Long, ED>> edgeTypeInfo =
      new TupleTypeInfo(Edge.class, BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO, TypeExtractor.createTypeInfo(
          epgmStore.getEdgeDataHandler().getEdgeDataFactory().getType()));
    // used for type hinting when loading graph data
    TypeInformation<Subgraph<Long, GD>> graphTypeInfo =
      new TupleTypeInfo(Subgraph.class, BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.createTypeInfo(
          epgmStore.getGraphDataHandler().getGraphDataFactory().getType()));

    DataSet<Vertex<Long, VD>> vertexDataSet = env.createInput(
      new HBaseReader.VertexDataTableInputFormat<>(
        epgmStore.getVertexDataHandler(), epgmStore.getVertexDataTableName()),
      vertexTypeInfo);

    DataSet<Edge<Long, ED>> edgeDataSet = env.createInput(
      new HBaseReader.EdgeDataTableInputFormat<>(epgmStore.getEdgeDataHandler(),
        epgmStore.getEdgeDataTableName()), edgeTypeInfo);

    DataSet<Subgraph<Long, GD>> subgraphDataSet = env.createInput(
      new HBaseReader.GraphDataTableInputFormat<>(
        epgmStore.getGraphDataHandler(), epgmStore.getGraphDataTableName()),
      graphTypeInfo);

    return new EPGMDatabase<>(
      vertexDataSet.map(new MapFunction<Vertex<Long, VD>, VD>() {
        @Override
        public VD map(Vertex<Long, VD> longVDVertex) throws Exception {
          return longVDVertex.getValue();
        }
      }).withForwardedFields("f1->*"),
      edgeDataSet.map(new MapFunction<Edge<Long, ED>, ED>() {
        @Override
        public ED map(Edge<Long, ED> longEDEdge) throws Exception {
          return longEDEdge.getValue();
        }
      }).withForwardedFields("f2->*"),
      subgraphDataSet.map(new MapFunction<Subgraph<Long, GD>, GD>() {
        @Override
        public GD map(Subgraph<Long, GD> longGDSubgraph) throws Exception {
          return longGDSubgraph.getValue();
        }
      }).withForwardedFields("f1->*"),
      epgmStore.getVertexDataHandler().getVertexDataFactory(),
      epgmStore.getEdgeDataHandler().getEdgeDataFactory(),
      epgmStore.getGraphDataHandler().getGraphDataFactory(), env);
  }

  /**
   * Returns a logical graph containing the complete vertex and edge space of
   * that EPGM database.
   *
   * @return logical graph of vertex and edge space
   */
  public LogicalGraph<VD, ED, GD> getDatabaseGraph() {
    return LogicalGraph
      .fromDataSets(database.getVertices(), database.getEdges(), databaseData,
        vertexDataFactory, edgeDataFactory, graphDataFactory);
  }

  /**
   * Returns a logical graph by its identifier.
   *
   * @param graphID graph identifier
   * @return logical graph or {@code null} if graph does not exist
   * @throws Exception
   */
  public LogicalGraph<VD, ED, GD> getGraph(Long graphID) throws Exception {
    return database.getGraph(graphID);
  }

  /**
   * Returns a collection of all logical graph contained in that EPGM database.
   *
   * @return collection of all logical graphs
   */
  public GraphCollection<VD, ED, GD> getCollection() {
    DataSet<VD> newVertices =
      database.getVertices()
        .filter(new FilterFunction<VD>() {
          @Override
          public boolean filter(VD vertex) throws
            Exception {
            return vertex.getGraphCount() > 0;
          }
        });
    DataSet<ED> newEdges = database.getEdges()
      .filter(new FilterFunction<ED>() {
        @Override
        public boolean filter(ED longEDEdge) throws Exception {
          return longEDEdge.getGraphCount() > 0;
        }
      });

    return new GraphCollection<>(newVertices, newEdges,
      database.getGraphHeads(), database.getVertexDataFactory(),
      database.getEdgeDataFactory(), database.getGraphDataFactory(), env);
  }
}