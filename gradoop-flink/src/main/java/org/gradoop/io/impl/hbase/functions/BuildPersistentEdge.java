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

package org.gradoop.io.impl.hbase.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.epgm.Edge;
import org.gradoop.model.api.epgm.Vertex;
import org.gradoop.storage.api.PersistentEdge;
import org.gradoop.storage.api.PersistentEdgeFactory;

/**
 * Creates persistent edge data objects from edge data and source/target
 * vertex data
 */
public class BuildPersistentEdge
  implements JoinFunction<Tuple2<Vertex, Edge>, Vertex, PersistentEdge> {

  /**
   * Persistent edge data factory.
   */
  private final PersistentEdgeFactory edgeFactory;

  /**
   * Creates join function
   *
   * @param edgeFactory persistent edge data factory.
   */
  public BuildPersistentEdge(PersistentEdgeFactory edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PersistentEdge join(
    Tuple2<Vertex, Edge> sourceVertexAndEdge, Vertex targetVertex)
    throws Exception {
    return edgeFactory.createEdge(sourceVertexAndEdge.f1,
      sourceVertexAndEdge.f0, targetVertex);
  }
}
