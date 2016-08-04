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
import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphTransaction;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Describes all operators that can be applied on a single logical graph in the
 * EPGM.
 */
public interface GraphTransactions {

  /**
   * Getter.
   * @return data set of graph transactions
   */
  DataSet<GraphTransaction> getTransactions();

  GradoopConfig<GraphHead, Vertex, Edge> getConfig();
}