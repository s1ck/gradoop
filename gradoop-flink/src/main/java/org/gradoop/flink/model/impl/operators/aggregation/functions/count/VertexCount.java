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

package org.gradoop.flink.model.impl.operators.aggregation.functions.count;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.functions.AggregateFunction;
import org.gradoop.common.model.api.functions.ApplyAggregateFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.FlinkGraphCollection;
import org.gradoop.flink.model.impl.FlinkLogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ToPropertyValue;
import org.gradoop.flink.model.impl.functions.graphcontainment.ExpandGraphsToIds;
import org.gradoop.flink.model.impl.operators.aggregation.functions.GroupCountToPropertyValue;
import org.gradoop.flink.model.impl.operators.count.Count;

/**
 * Aggregate function returning the vertex count of a graph / graph collection.
 */
public class VertexCount implements AggregateFunction<GraphHead, Vertex, Edge, FlinkLogicalGraph,
    FlinkGraphCollection, DataSet<PropertyValue>, DataSet<Boolean>>,ApplyAggregateFunction<GraphHead, Vertex, Edge, FlinkLogicalGraph,
        FlinkGraphCollection, DataSet<Tuple2<GradoopId, PropertyValue>>, DataSet<Boolean>> {

  /**
   * Returns a 1-element dataset containing the vertex count of the given graph.
   *
   * @param graph input graph
   * @return 1-element dataset with vertex count
   */
  @Override
  public DataSet<PropertyValue> execute(FlinkLogicalGraph graph) {
    return Count
      .count(graph.getVertices())
      .map(new ToPropertyValue<Long>());
  }

  /**
   * Returns a dataset containing graph identifiers and the corresponding vertex
   * count.
   *
   * @param collection input graph collection
   * @return dataset with graph + vertex count tuples
   */
  @Override
  public DataSet<Tuple2<GradoopId, PropertyValue>> execute(
    FlinkGraphCollection collection) {
    return Count.groupBy(collection.getVertices()
      .flatMap(new ExpandGraphsToIds<Vertex>())
    ).map(new GroupCountToPropertyValue());
  }

  /**
   * Return default property value, in this case 0L.
   * @return default property value of this aggregation function
   */
  @Override
  public Number getDefaultValue() {
    return 0L;
  }
}
