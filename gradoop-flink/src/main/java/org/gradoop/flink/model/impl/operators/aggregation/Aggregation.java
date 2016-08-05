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

package org.gradoop.flink.model.impl.operators.aggregation;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.functions.AggregateFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.FlinkGraphCollection;
import org.gradoop.flink.model.impl.FlinkLogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.PropertySetterBroadcast;
import org.gradoop.flink.model.impl.operators.FlinkUnaryGraphToGraphOperator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Takes a logical graph and a user defined aggregate function as input. The
 * aggregate function is applied on the logical graph and the resulting
 * aggregate is stored as an additional property at the result graph.
 */
public class Aggregation implements FlinkUnaryGraphToGraphOperator {

  /**
   * Used to store aggregate result.
   */
  private final String aggregatePropertyKey;

  /**
   * User-defined aggregate function which is applied on a single logical graph.
   */
  private final AggregateFunction<GraphHead, Vertex, Edge, FlinkLogicalGraph,
    FlinkGraphCollection, DataSet<PropertyValue>, DataSet<Boolean>>
    aggregateFunction;

  /**
   * Creates new aggregation.
   *
   * @param aggregatePropertyKey property key to store result of
   *                             {@code aggregateFunction}
   * @param aggregateFunction    user defined aggregation function which gets
   *                             called on the input graph
   */
  public Aggregation(final String aggregatePropertyKey,
    final AggregateFunction<GraphHead, Vertex, Edge, FlinkLogicalGraph,
      FlinkGraphCollection, DataSet<PropertyValue>, DataSet<Boolean>>
      aggregateFunction) {
    this.aggregatePropertyKey = checkNotNull(aggregatePropertyKey);
    this.aggregateFunction = checkNotNull(aggregateFunction);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FlinkLogicalGraph execute(FlinkLogicalGraph graph) {

    DataSet<PropertyValue> aggregateValue = aggregateFunction.execute(graph);

    DataSet<GraphHead> graphHead = graph.getGraphHead()
      .map(new PropertySetterBroadcast<GraphHead>(aggregatePropertyKey))
      .withBroadcastSet(aggregateValue, PropertySetterBroadcast.VALUE);

    return FlinkLogicalGraph.fromDataSets(
      graphHead,
      graph.getVertices(),
      graph.getEdges(),
      graph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Aggregation.class.getName();
  }
}
