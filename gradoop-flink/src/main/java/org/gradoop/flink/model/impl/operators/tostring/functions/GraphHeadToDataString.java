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
package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.gradoop.flink.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;
import org.gradoop.common.model.api.entities.EPGMGraphHead;

/**
 * represents a graph head by a data string (label and properties)
 * @param <G> graph head type
 */
public class GraphHeadToDataString<G extends EPGMGraphHead>
  extends EPGMElementToDataString<G> implements GraphHeadToString<G> {

  @Override
  public GraphHeadString map(G graph) throws Exception {
    return new GraphHeadString(graph.getId(), "|" + label(graph) + "|");
  }
}
