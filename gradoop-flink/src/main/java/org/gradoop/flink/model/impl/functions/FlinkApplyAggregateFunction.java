package org.gradoop.flink.model.impl.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.functions.ApplyAggregateFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.FlinkGraphCollection;
import org.gradoop.flink.model.impl.FlinkLogicalGraph;

/**
 * Base class for aggregate functions that can be used with
 * {@link org.gradoop.flink.model.impl.operators.aggregation.ApplyAggregation}.
 */
public abstract class FlinkApplyAggregateFunction implements
  ApplyAggregateFunction<GraphHead, Vertex, Edge,
    FlinkLogicalGraph, FlinkGraphCollection,
    DataSet<Tuple2<GradoopId, PropertyValue>>, DataSet<Boolean>> {
}
