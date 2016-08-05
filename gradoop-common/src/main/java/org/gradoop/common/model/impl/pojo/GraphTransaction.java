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

package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.api.entities.EPGMGraphTransaction;

import java.io.Serializable;
import java.util.Set;

/**
 * An encapsulated representation of a logical graph with duplicated elements.
 */
public class GraphTransaction implements EPGMGraphTransaction
  <GraphHead, Vertex, Edge>, Serializable {

  private GraphHead graphHead;

  private Set<Vertex> vertices;

  private Set<Edge> edges;

  /**
   * default constructor
   */
  public GraphTransaction() {
  }

  /**
   * valued constructor
   * @param graphHead graph head
   * @param vertices set of vertices
   * @param edges set of edges
   */
  public GraphTransaction(GraphHead graphHead, Set<Vertex> vertices,
    Set<Edge> edges) {
    setGraphHead(graphHead);
    setVertices(vertices);
    setEdges(edges);
  }

  public GraphHead getGraphHead() {
    return this.graphHead;
  }

  public void setGraphHead(GraphHead graphHead) {
    this.graphHead = graphHead;
  }

  public Set<Vertex> getVertices() {
    return this.vertices;
  }

  public void setVertices(Set<Vertex> vertices) {
    this.vertices = vertices;
  }

  public Set<Edge> getEdges() {
    return this.edges;
  }

  public void  setEdges(Set<Edge> edges) {
    this.edges = edges;
  }
}
