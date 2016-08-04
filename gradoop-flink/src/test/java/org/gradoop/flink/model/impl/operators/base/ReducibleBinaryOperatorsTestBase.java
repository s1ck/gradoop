package org.gradoop.flink.model.impl.operators.base;

import org.gradoop.common.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.flink.model.impl.FlinkGraphCollection;
import org.gradoop.flink.model.impl.FlinkLogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;

import static org.junit.Assert.assertTrue;

public abstract class ReducibleBinaryOperatorsTestBase extends BinaryGraphOperatorsTestBase {

  protected void checkExpectationsEqualResults(FlinkAsciiGraphLoader loader,
    UnaryCollectionToGraphOperator operator) throws Exception {
    // overlap
    FlinkGraphCollection col13 = loader.getGraphCollectionByVariables("g1", "g3");

    FlinkLogicalGraph exp13 = loader.getLogicalGraphByVariable("exp13");

    // no overlap
    FlinkGraphCollection col12 = loader.getGraphCollectionByVariables("g1", "g2");

    FlinkLogicalGraph exp12 = loader.getLogicalGraphByVariable("exp12");

    // full overlap
    FlinkGraphCollection col14 = loader.getGraphCollectionByVariables("g1", "g4");

    FlinkLogicalGraph exp14 = loader.getLogicalGraphByVariable("exp14");

    assertTrue("partial overlap failed",
      operator.execute(col13).equalsByElementData(exp13).collect().get(0));
    assertTrue("without overlap failed",
      operator.execute(col12).equalsByElementData(exp12).collect().get(0));
    assertTrue("with full overlap failed",
      operator.execute(col14).equalsByElementData(exp14).collect().get(0));
  }
}
