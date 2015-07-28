package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.impl.operators.io.formats.FlinkBTGVertexType;
import org.gradoop.model.impl.operators.io.formats.FlinkBTGVertexValue;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class FlinkBTGAlgorithmTestHelper {
  /**
   * Used for splitting the line into the main tokens (vertex id, vertex value,
   * edges)
   */
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile("[,]");
  /**
   * Used for splitting a main token into its values (vertex value = type,
   * value, btg-ids; edge list)
   */
  private static final Pattern VALUE_TOKEN_SEPARATOR = Pattern.compile("[ ]");

  public static DataSet<Vertex<Long, FlinkBTGVertexValue>>
  getConnectedIIGVertices(
    String[] graph, ExecutionEnvironment env) {
    List<Vertex<Long, FlinkBTGVertexValue>> vertices = new ArrayList<>();
    for (String line : graph) {
      String[] lineTokens = LINE_TOKEN_SEPARATOR.split(line);
      long id = Long.parseLong(lineTokens[0]);
      String[] valueTokens = VALUE_TOKEN_SEPARATOR.split(lineTokens[1]);
      FlinkBTGVertexType vertexClass =
        FlinkBTGVertexType.values()[Integer.parseInt(valueTokens[0])];
      Double vertexValue = Double.parseDouble(valueTokens[1]);
      List<Long> btgIDs =
        Lists.newArrayListWithCapacity(valueTokens.length - 1);
      for (int n = 2; n < valueTokens.length; n++) {
        btgIDs.add(Long.parseLong(valueTokens[n]));
      }
      vertices.add(new Vertex<>(id,
        new FlinkBTGVertexValue(vertexClass, vertexValue, btgIDs)));
      System.out.println("Erzeuge Knoten:");
      System.out.println("id:" + id);
      System.out.println("vertexClass:" + vertexClass);
      System.out.println("vertexValue:" + vertexValue);
      System.out.println("BtgIDS" + btgIDs);
    }
    return env.fromCollection(vertices);
  }

  public static DataSet<Edge<Long, Long>> getConnectedIIGEdges(String[] graph,
    ExecutionEnvironment env) {
    List<Edge<Long, Long>> edges = new ArrayList<>();
    for (String line : graph) {
      String[] lineTokens = LINE_TOKEN_SEPARATOR.split(line);
      long id = Long.parseLong(lineTokens[0]);
      String[] edgeTokens =
        (lineTokens.length == 3) ? VALUE_TOKEN_SEPARATOR.split(lineTokens[2]) :
          new String[0];
      for (String edgeToken : edgeTokens) {
        long tar = Long.parseLong(edgeToken);
        edges.add(new Edge<Long, Long>(id, tar, 0L));
        System.out.println("SRC ---> Tar" + id + " -->" + tar);
      }
    }
    return env.fromCollection(edges);
  }
}

