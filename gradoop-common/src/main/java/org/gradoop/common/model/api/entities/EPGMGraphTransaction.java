package org.gradoop.common.model.api.entities;

import java.util.Set;

/**
 * A graph transaction represents  a graph including its meta data (graph head),
 * vertices and edges.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public interface EPGMGraphTransaction
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {

  G getGraphHead();

  void setGraphHead(G graphHead);

  Set<V> getVertices();

  void setVertices(Set<V> vertices);

  Set<E> getEdges();

  void setEdges(Set<E> edges);
}
