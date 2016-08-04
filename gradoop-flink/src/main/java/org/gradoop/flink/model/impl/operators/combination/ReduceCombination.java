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

package org.gradoop.flink.model.impl.operators.combination;

import org.gradoop.common.model.api.operators.GraphCollection;
import org.gradoop.common.model.api.operators.LogicalGraph;
import org.gradoop.flink.model.impl.FlinkLogicalGraph;
import org.gradoop.common.model.api.operators.ReducibleBinaryGraphToGraphOperator;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Computes the combined graph from a collection of logical graphs.
 */
public class ReduceCombination implements ReducibleBinaryGraphToGraphOperator {

  /**
   * Creates a new logical graph by union the vertex and edge sets of all graph
   * contained in the given collection.
   *
   * @param collection input collection
   * @return combined graph
   */
  @Override
  public LogicalGraph execute(GraphCollection collection) {
    return FlinkLogicalGraph.fromDataSets(
      collection.getVertices(),
      collection.getEdges(), (GradoopFlinkConfig) collection.getConfig());
  }

  @Override
  public String getName() {
    return ReduceCombination.class.getName();
  }
}
