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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.algorithms.fsm.gspan.decoders;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.algorithms.fsm.config.BroadcastNames;
import org.gradoop.algorithms.fsm.gspan.api.GSpanDecoder;
import org.gradoop.algorithms.fsm.gspan.decoders.functions.DFSDecoder;
import org.gradoop.algorithms.fsm.gspan.decoders.functions.EdgeLabelDecoder;
import org.gradoop.algorithms.fsm.gspan.decoders.functions.ExpandEdges;
import org.gradoop.algorithms.fsm.gspan.decoders.functions.ExpandVertices;
import org.gradoop.algorithms.fsm.gspan.decoders.functions.FullEdge;
import org.gradoop.algorithms.fsm.gspan.decoders.functions.FullVertex;
import org.gradoop.algorithms.fsm.gspan.decoders.functions.VertexLabelDecoder;
import org.gradoop.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of3;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Turns the gSpan result into a EPGM graph collection
 */
public class GSpanGraphCollectionDecoder implements 
  GSpanDecoder<GraphCollection> {

  /**
   * Gradoop configuration
   */
  private final GradoopFlinkConfig gradoopConfig;

  /**
   * Constructor.
   *
   * @param gradoopFlinkConfig Gradoop configuration
   */
  public GSpanGraphCollectionDecoder(
    GradoopFlinkConfig gradoopFlinkConfig) {

    this.gradoopConfig = gradoopFlinkConfig;
  }

  @Override
  public GraphCollection decode(
    DataSet<WithCount<CompressedDFSCode>> frequentSubgraphs,
    DataSet<List<String>> vertexLabelDictionary,
    DataSet<List<String>> edgeLabelDictionary) {


    DataSet<Tuple3<GraphHead, ArrayList<Tuple2<GradoopId, Integer>>,
      ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>> graphTriples =
      frequentSubgraphs
        .map(new DFSDecoder<>(gradoopConfig.getGraphHeadFactory()));

    DataSet<GraphHead> graphHeads = graphTriples
      .map(new Value0Of3<GraphHead, ArrayList<Tuple2<GradoopId, Integer>>,
        ArrayList<Tuple3<GradoopId, GradoopId, Integer>>>());

    DataSet<Vertex> vertices = graphTriples
      .flatMap(new ExpandVertices<GraphHead>())
      .map(new VertexLabelDecoder())
      .withBroadcastSet(vertexLabelDictionary, BroadcastNames.VERTEX_DICTIONARY)
      .map(new FullVertex<>(gradoopConfig.getVertexFactory()))
      .returns(gradoopConfig.getVertexFactory().getType());

    DataSet<Edge> edges = graphTriples
      .flatMap(new ExpandEdges<GraphHead>())
      .map(new EdgeLabelDecoder())
      .withBroadcastSet(edgeLabelDictionary, BroadcastNames.EDGE_DICTIONARY)
      .map(new FullEdge<>(gradoopConfig.getEdgeFactory()))
      .returns(gradoopConfig.getEdgeFactory().getType());

    return GraphCollection
      .fromDataSets(graphHeads, vertices, edges, gradoopConfig);
  }
}