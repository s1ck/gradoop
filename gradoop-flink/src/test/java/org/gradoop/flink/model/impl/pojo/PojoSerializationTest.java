package org.gradoop.flink.model.impl.pojo;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EdgePojoFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadPojoFactory;
import org.gradoop.common.model.impl.pojo.VertexPojoFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.gradoop.flink.model.impl.GradoopFlinkTestUtils.writeAndRead;
import static org.junit.Assert.assertEquals;

public class PojoSerializationTest extends GradoopFlinkTestBase {

  @Test
  public void testVertexSerialization() throws Exception {
    EPGMVertex vertexIn = new VertexPojoFactory().createVertex(
      "Person",
      PropertyList.createFromMap(GradoopTestUtils.SUPPORTED_PROPERTIES),
      GradoopIdSet.fromExisting(GradoopId.get()));

    Assert.assertEquals("EPGMVertex POJOs were not equal",
      vertexIn, GradoopFlinkTestUtils.writeAndRead(vertexIn));
  }

  @Test
  public void testEdgeSerialization() throws Exception {
    EPGMEdge edgeIn = new EdgePojoFactory().createEdge(
      "knows",
      GradoopId.get(),
      GradoopId.get(),
      PropertyList.createFromMap(GradoopTestUtils.SUPPORTED_PROPERTIES),
      GradoopIdSet.fromExisting(GradoopId.get(), GradoopId.get()));

    Assert.assertEquals("EPGMEdge POJOs were not equal",
      edgeIn, GradoopFlinkTestUtils.writeAndRead(edgeIn));
  }

  @Test
  public void testGraphHeadSerialization() throws Exception {
    EPGMGraphHead graphHeadIn = new GraphHeadPojoFactory().createGraphHead(
      "Community",
      PropertyList.createFromMap(GradoopTestUtils.SUPPORTED_PROPERTIES)
    );

    Assert.assertEquals("EPGMGraphHead POJOs were not equal",
      graphHeadIn, GradoopFlinkTestUtils.writeAndRead(graphHeadIn));
  }


}
