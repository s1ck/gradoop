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

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMGraphTransaction;
import org.gradoop.common.model.api.entities.EPGMVertex;

/**
 * Generates a set of graph transactions
 */
public interface GraphTransactionsGenerator
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge,
    GT extends EPGMGraphTransaction<G, V, E>>
  extends Operator {

  /**
   * generates the graph transactions
   * @return graph collection
   */
  GraphTransactions<G, V, E, GT> execute();
}
