package org.gradoop.flink.model.impl.operators.limit;

import org.apache.flink.api.common.InvalidProgramException;
import org.gradoop.common.model.api.operators.GraphCollection;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.FlinkGraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LimitTest extends GradoopFlinkTestBase {

  @Test
  public void testInBound() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    int limit = 2;

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    GraphCollection outputCollection = inputCollection.limit(limit);

    assertEquals(limit, outputCollection.getGraphHeads().count());
  }

  @Test
  public void testOutOfBound() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    FlinkGraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    int limit = 4;
    int expectedLimit = 2;

    GraphCollection outputCollection = inputCollection.limit(limit);

    assertEquals(expectedLimit, outputCollection.getGraphHeads().count());
  }

  @Test
  public void testEmpty() throws Exception {
    FlinkGraphCollection inputCollection =
      FlinkGraphCollection.createEmptyCollection(
        GradoopFlinkConfig.createConfig(getExecutionEnvironment()));

    int limit = 4;
    int expectedCount = 0;

    GraphCollection outputCollection = inputCollection.limit(limit);

    assertEquals(expectedCount, outputCollection.getGraphHeads().count());
  }

  @Test(expected = InvalidProgramException.class)
  public void testNegativeLimit() throws Exception {
    FlinkGraphCollection inputCollection =
      FlinkGraphCollection.createEmptyCollection(
        GradoopFlinkConfig.createConfig(getExecutionEnvironment()));

    int limit = -1;
    int expectedCount = 0;

    GraphCollection outputCollection = inputCollection.limit(limit);

    assertEquals(expectedCount, outputCollection.getGraphHeads().count());
  }
}
