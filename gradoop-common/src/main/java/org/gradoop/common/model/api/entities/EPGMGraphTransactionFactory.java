package org.gradoop.common.model.api.entities;

import java.util.Set;

/**
 * Initializes {@link EPGMGraphTransaction} objects of a given type.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <GT> EPGM graph transaction type
 */
public interface EPGMGraphTransactionFactory
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge,
    GT extends EPGMGraphTransaction<G, V, E>> extends EPGMElementFactory<GT> {

  GT initGraphTransaction(G graphHead, Set<V> vertices, Set<E> edges);
}
