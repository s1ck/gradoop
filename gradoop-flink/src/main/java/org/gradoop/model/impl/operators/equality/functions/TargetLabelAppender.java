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

package org.gradoop.model.impl.operators.equality.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.operators.equality.tuples.EdgeDataLabel;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;

/**
 * "edgeLabel","targetLabel" => "edgeLabel[targetLabel]"
 */
public class TargetLabelAppender
  extends VertexLabelAppender
  implements JoinFunction<EdgeDataLabel, DataLabel, DataLabel> {

  @Override
  public DataLabel join(
    EdgeDataLabel edgeLabel, DataLabel targetLabel
  ) throws Exception {

    return new DataLabel(edgeLabel.getGraphId(),
      edgeLabel.getSourceId(), label(targetLabel, edgeLabel)
    );
  }
}