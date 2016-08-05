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

package org.gradoop.common.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.hadoop.yarn.state.Graph;
import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.io.api.DataSink;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.io.IOException;

/**
 * Operators that are available at all graph structures.
 *
 * @see LogicalGraph
 * @see GraphCollection
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public interface GraphBaseOperators
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {

  //----------------------------------------------------------------------------
  // Containment methods
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------


  DataSet<Boolean> isEmpty();

  /**
   * Returns the config attached to that graph / graph collection.
   *
   * @return gradoop config
   */
  GradoopConfig<G, V, E> getConfig();

  /**
   * Writes logical graph/graph collection to given data sink.
   *
   * @param dataSink data sing
   */
  void writeTo(DataSink dataSink) throws IOException;
}
