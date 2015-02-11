import com.clearspring.analytics.stream.cardinality.HyperLogLog
import org.apache.spark.graphx._

import scala.collection.mutable._
import scala.reflect.ClassTag

/** HyperANF algorithm. */
object HyperAnf {
  /**
   * Compute the Approximation of the Neighbourhood Function of a graph
   * The Neighbourhood Function N(t) counts the number of pairs of nodes such that each one is reachable from other in less than t hops
   * See "Boldi P, Rosa M, Vigna S. HyperANF: Approximating the neighbourhood function of very large graphs on a budget[C]//Proceedings of the 20th international conference on World wide web. ACM, 2011: 625-634."
   *
   * @tparam VD the vertex attribute type (discarded in the computation)
   * @tparam ED the edge attribute type (discarded in the computation)
   *
   * @param graph the graph for which to compute the neighbourhood function
   * @param log2m number of registers, larger leads to better estimate but much costly
   *
   * @return a list of number that its t-th element counts the approximate number of N(t)
   */
  def anf[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], log2m: Int): MutableList[Double] = {
    // Initial the graph, assign a counter to each vertex that contains the vertex id only
    var anfGraph = graph.mapVertices { case (vid, _) =>
      val counter = new HyperLogLog(log2m)
      counter.offer(vid)
      counter
    }

    // Send counter to neighbourhood
    def sendMessage(edge: EdgeTriplet[HyperLogLog, ED]) = {
      Iterator((edge.srcId, edge.dstAttr), (edge.dstId, edge.srcAttr))
    }

    // Merge two counters of different neighbourhood
    def mergeMessage(msg1: HyperLogLog, msg2: HyperLogLog): HyperLogLog = {
      mergeCounter(msg1, msg2)
    }

    // Merge the counter from neighbourhood to the counter of the vertex
    def vertexProgram(vid: VertexId, attr: HyperLogLog, msg: HyperLogLog): HyperLogLog = {
      mergeCounter(attr, msg)
    }

    // Initial message and neighbourhood function list
    val initialMessage = new HyperLogLog(log2m)

    val neighbourhoodFunction = new MutableList[Double]

    // It should be the approximation of number vertices of the graph
    var anf = anfGraph.vertices.map{ case(vid, counter) => counter.cardinality() }.reduce(_ + _)
    neighbourhoodFunction += anf

    // Start iteration like pregel
    var prevG: Graph[HyperLogLog, ED] = null
    var messages = anfGraph.mapReduceTriplets(sendMessage, mergeMessage)

    var lastAnf = 0.0
    // Loop until the neighbourhood function list does not increase
    while(neighbourhoodFunction.isEmpty || lastAnf != anf) {

      lastAnf = anf

      // Update vertices
      val newVerts = anfGraph.vertices.innerJoin(messages)(vertexProgram).cache()
      prevG = anfGraph
      anfGraph = anfGraph.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
      anfGraph.cache()
      anf = anfGraph.vertices.map{ case(vid, counter) => counter.cardinality() }.reduce(_ + _)

      neighbourhoodFunction += anf

      // Propagation
      val oldMessages = messages
      messages = anfGraph.mapReduceTriplets(sendMessage, mergeMessage, Some((newVerts, EdgeDirection.Either))).cache()
      messages.count()

      // Unpersist
      oldMessages.unpersist(blocking=false)
      newVerts.unpersist(blocking=false)
      prevG.unpersistVertices(blocking=false)
      prevG.edges.unpersist(blocking=false)
    }

    neighbourhoodFunction
  }

  /**
   * Calculate the average distance of a graph
   * @param anf Approximate neighbourhood function
   * @return Average distance
   */
  def avgDistance(anf: MutableList[Double]): Double = {
    val maxANF = anf.max
    val cdf = anf.map(x => x / maxANF)
    val size = anf.size
    val pdf = cdf.takeRight(size-1) zip cdf.take(size-1) map {case (next, cur) => (next - cur) / (1 - cdf(0)) }
    1 to size zip pdf map {case (distance, p) => distance * p} sum
  }

  /**
   * Merge two counters
   * @param hll1 HyperLogLog counter
   * @param hll2 Another HyperLogLog counter
   * @return Merged conter contains elements of two input counters
   */
  def mergeCounter(hll1: HyperLogLog, hll2: HyperLogLog): HyperLogLog = {
    hll1.merge(hll2) match {
      case hll: HyperLogLog => hll
      case _ => throw new ClassCastException
    }
  }
}
