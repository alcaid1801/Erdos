import org.apache.spark._
import org.apache.spark.graphx._
import org.scalatest.FunSuite

class HyperANFSuite extends FunSuite {
  test("HyperAnf on Simple Graph") {
    val sc = new SparkContext("local", "my test app", new SparkConf)
    val edge_path = "src/test/resources/graph.edge"
    val graph = GraphLoader.edgeListFile(sc, edge_path)
    val anf = HyperAnf.anf(graph, 7)
    assert(HyperAnf.avgDistance(anf) === 32.0/15)
  } // end of HyperAnf
}
