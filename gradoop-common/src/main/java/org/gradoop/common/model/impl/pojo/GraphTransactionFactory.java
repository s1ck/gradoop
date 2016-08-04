package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.api.entities.EPGMGraphTransaction;
import org.gradoop.common.model.api.entities.EPGMGraphTransactionFactory;

import java.io.Serializable;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Factory for creating graph transaction POJOs.
 */
public class GraphTransactionFactory implements
  EPGMGraphTransactionFactory<GraphHead, Vertex, Edge, GraphTransaction>,
  Serializable {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  @Override
  public GraphTransaction initGraphTransaction(GraphHead graphHead,
    Set<Vertex> vertices, Set<Edge> edges) {
    checkNotNull(graphHead, "graph head must not be null");
    checkNotNull(vertices, "vertices must not be null");
    checkNotNull(edges, "edges must not be null");
    return new GraphTransaction(graphHead, vertices, edges);
  }

  @Override
  public Class<GraphTransaction> getType() {
    return GraphTransaction.class;
  }
}
