package dmitry.examples.flink.graph

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.graph.scala._
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.ml.preprocessing.Splitter
import org.apache.flink.types.NullValue
import org.apache.flink.util.Collector


/**
  * Created by dima on 2/15/17.
  *
  * Gelly experiments.
  */
object GraphExample extends App {
    val env = ExecutionEnvironment.getExecutionEnvironment


    // Edges valued
    val edges = env.fromElements(new Edge(1L, 2L, "Edge 1-2"),
      new Edge(1L, 3L, "Edge 1-3"),
      new Edge(3L, 4L, "Edge 3-4"),
      new Edge(4L, 5L, "Edge 5-4")
    ).map(v => (v.getSource, v.getTarget))

    // Vertices valued
    val vertices = env.fromElements(new Vertex(1L, "Vertex 1"),
      new Vertex(2L, "Vertex 2"),
      new Vertex(3L, "Vertex 3"),
      new Vertex(4L, "Vertex 4"),
      new Vertex(5L, "Vertex 5")
    ).map{ v => (v.getId, v.getId)}//.withForwardedFields("*->_1;*->_2")

    val collection = vertices.collect()

    // Build graph
    //val graph = Graph.fromDataSet(vertices, edges,env)
    val maxIterations = 10
  // open a delta iteration
  val verticesWithComponents = vertices.iterateDelta(vertices, maxIterations, Array("_1")) {
    (s, ws) =>

      // apply the step logic: join with the edges
      val allNeighbors = ws.join(edges).where(0).equalTo(0) { (vertex, edge) =>
        (edge._2, vertex._2)
      }//.withForwardedFieldsFirst("_2->_2").withForwardedFieldsSecond("_2->_1")

      // select the minimum neighbor
      val minNeighbors = allNeighbors.groupBy(0).min(1)

      // update if the component of the candidate is smaller
      val updatedComponents = minNeighbors.join(s).where(0).equalTo(0) {
        (newVertex, oldVertex, out: Collector[(Long, Long)]) =>
          if (newVertex._2 < oldVertex._2) out.collect(newVertex)
      }//.withForwardedFieldsFirst("*")

      // delta and new workset are identical
      (updatedComponents, updatedComponents)
  }

 verticesWithComponents.print()



}
