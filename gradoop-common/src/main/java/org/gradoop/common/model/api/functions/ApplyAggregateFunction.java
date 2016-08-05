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

package org.gradoop.common.model.api.functions;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.operators.GraphCollection;
import org.gradoop.common.model.api.operators.LogicalGraph;

/**
 * Describes an aggregate function that can be applied on a collection of graphs
 * and computes an aggregate value for each graph contained in the collection.
 *
 * @see ApplyAggregateFunction
 */
public interface ApplyAggregateFunction
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge,
    LG extends LogicalGraph<G, V, E, LG, GC, AGG_OUT, BOOL_OUT>,
    GC extends GraphCollection<G, V, E, LG, GC, AGG_OUT, BOOL_OUT>,
    AGG_OUT, BOOL_OUT> {

  /**
   * Defines the aggregate function. The input is a graph collection, the output
   * contains a tuple for each graph contained in the collection. The tuple
   * holds the graph identifier and the associated aggregate value (e.g. count).
   *
   * @param collection input graph collection
   * @return aggregate values for all graphs
   */
  AGG_OUT execute(GC collection);

  /**
   * Return the default value that will be used when a graph has no vertices
   * or edges with the specified property.
   * @return default value
   */
  Number getDefaultValue();
}
