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

package org.gradoop.model.impl.algorithms.labelpropagation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.api.entities.EPGMEdge;

/**
 * Maps EPGM edge to a Gelly edge consisting of EPGM source and target
 * identifier and {@link NullValue} as edge value.
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFields("sourceId->f0;targetId->f1")
public class EdgeToGellyEdgeMapper<E extends EPGMEdge>
  implements MapFunction<E, org.apache.flink.graph.Edge> {
  /**
   * Reduce object instantiations
   */
  private final org.apache.flink.graph.Edge reuseEdge;

  /**
   * Constructor
   */
  public EdgeToGellyEdgeMapper() {
    reuseEdge = new org.apache.flink.graph.Edge();
    reuseEdge.setValue(NullValue.getInstance());
  }

  @Override
  public org.apache.flink.graph.Edge map(E epgmEdge) throws Exception {
    reuseEdge.setSource(epgmEdge.getSourceId());
    reuseEdge.setTarget(epgmEdge.getTargetId());
    return reuseEdge;
  }
}
