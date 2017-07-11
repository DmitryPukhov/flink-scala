package dmitry.examples.flink.graph

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.graph.scala._
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.ml.preprocessing.Splitter
import org.apache.flink.types.NullValue


/**
  * Created by dima on 2/15/17.
  *
  * Gelly experiments.
  */
object GraphExample {
  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment


    // Edges valued
    val edges = env.fromElements(new Edge(1L, 2L, "Edge 1-2"),
      new Edge(1L, 3L, "Edge 1-3"),
      new Edge(3L, 4L, "Edge 3-4"),
      new Edge(5L, 5L, "Edge 5-5")
    )

    // Vertices valued
    val vertices = env.fromElements(new Vertex(1L, "Vertex 1"),
      new Vertex(2L, "Vertex 2"),
      new Vertex(3L, "Vertex 3"),
      new Vertex(4L, "Vertex 3"),
      new Vertex(5L, "Vertex 5"))

    val collection = vertices.collect()

    // Build graph
    val graph = Graph.fromDataSet(vertices, edges,env)

    print(s"vertices=${graph.numberOfVertices()}")
    print(s"edges=${graph.numberOfEdges()}")

//    val graph2 = graph.mapVertices(v => v.getValue + "_v2")
//      .mapEdges(e => e.getValue + "_e2")
//      .subgraph(v => v.getId > 1, e => e.getSource > 1)
//      .addVertex(new Vertex(6L, "Vertex 6"))
//      .addEdge(vertices.collect()(0), new Vertex(7L, "Vertex7"), "New edge1-7")
//
//    // Do actions on graph
//    graph2.getTriplets.print()

  }
}
