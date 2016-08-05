package org.gradoop.flink.model.impl.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.FlinkGraphCollection;
import org.gradoop.flink.model.impl.FlinkLogicalGraph;

/**
 * Base class for all Flink based implementations of
 * {@link BinaryCollectionToValueOperator}.
 *
 * @param <T> value type
 */
public interface FlinkBinaryCollectionToValueOperator<T> extends
  BinaryCollectionToValueOperator<GraphHead, Vertex, Edge,
    FlinkLogicalGraph, FlinkGraphCollection,
    DataSet<PropertyValue>, DataSet<Boolean>, DataSet<T>>
  {
}
