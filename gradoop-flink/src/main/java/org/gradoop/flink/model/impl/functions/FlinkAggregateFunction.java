package org.gradoop.flink.model.impl.functions;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.functions.AggregateFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.FlinkGraphCollection;
import org.gradoop.flink.model.impl.FlinkLogicalGraph;

/**
 * Base class for aggregate functions that can be used with
 * {@link org.gradoop.flink.model.impl.operators.aggregation.Aggregation}.
 */
public interface FlinkAggregateFunction extends
  AggregateFunction<GraphHead, Vertex, Edge, FlinkLogicalGraph,
    FlinkGraphCollection, DataSet<PropertyValue>, DataSet<Boolean>> {
}
