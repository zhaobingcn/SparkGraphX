package com.spark.graphx

import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object ImprovedPageRank {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("pregel")

    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, "src/main/resources/test_web.txt")

    //**********************计算顶点的出度数A,并更新edge的值为1/A以及更新顶点为1

    val outDegrees = graph.outDegrees

    val outDegreesGraph = graph.outerJoinVertices(outDegrees) {
      (vId, vData, OptOutDegree) =>
        //更新顶点数据为出度
        OptOutDegree.getOrElse(0)
    }


    val weightedEdgesGraph = outDegreesGraph.mapTriplets { EdgeTriplet =>
      //边界的值
      //1/出度数目
      1.0 / EdgeTriplet.srcAttr
    }

    //重新把顶点数据设置为一
    val inputGraph = weightedEdgesGraph.mapVertices((id, vData) =>
      if(id < 4){
        new DataType(1.0, 1.0, false)
      }
      else{
        new DataType(1.0, 1.0, true)
      }

    )
    //**********************
    //**********************计算pageRanks**********************
    //1.所有顶点的消息初始化为0
    //2.遍历每个triplet,把出度数发射给目标对象
    //3.目标合并所有的出度数(每次都利用上次的Iterator)
    //4.updateVertex 最终处理
    val firstMessage = 0.0
    val iterations = 40
    val edgeDirection = EdgeDirection.Out

    val updateVertex = (vId: Long, vData: DataType, msgSum: Double) =>{
      if(vData.isNew == false){
        if(msgSum > 0.1) {
          new DataType(vData.nowData, vData.nowData + msgSum, false)
        }
        else {
          new DataType(vData.nowData, vData.nowData, false)
        }
      }
      else {
        if(math.abs(0.15 + 0.85 * msgSum - vData.nowData) > 0.01){
          new DataType(vData.nowData, 0.15 + 0.85 * msgSum, true)
        }
        else {
          new DataType(0.15 + 0.85 * msgSum, 0.15 + 0.85 * msgSum, true)
        }
      }
    }
    //triplet.srcAttr = 1
    val sendMsg = (triplet: EdgeTriplet[DataType, Double]) => {

      if(triplet.dstAttr.isNew == false && math.abs(triplet.srcAttr.prevData - triplet.srcAttr.nowData) > 0.01 )

        Iterator((triplet.dstId, math.abs(triplet.srcAttr.nowData - triplet.srcAttr.prevData) * triplet.attr))

      if(triplet.dstAttr.isNew == true && math.abs(triplet.srcAttr.prevData - triplet.srcAttr.nowData) > 0.01)

        Iterator((triplet.dstId, triplet.srcAttr.nowData * triplet.attr))

      else
        Iterator.empty

    }

    val aggregateMsgs = (x: Double, y: Double) => x + y

    val pw = new PrintWriter("src/main/resources/improvedPageRankLog.txt")

    val influenceGraph = pregel0[DataType, Double, Double](pw, inputGraph, firstMessage, iterations, edgeDirection)(updateVertex, sendMsg, aggregateMsgs)

    influenceGraph.vertices.foreach(a => println(a))

  }

  case class DataType(prevData: Double, nowData: Double, isNew: Boolean)


  def createLogGraph(sc : SparkContext): Unit ={
    val logNormalGraph = util.GraphGenerators.logNormalGraph(sc, 15)
    val pw = new PrintWriter("file/logNormalGraph.gexf")
    pw.write(toGexf(logNormalGraph))
    pw.close()
    logNormalGraph.aggregateMessages[Int](
      _.sendToSrc(1), _ + _).map(_._2).collect().sorted
  }

  def createRMatGraph(sc : SparkContext): Unit ={
    val pw =new java.io.PrintWriter("file/rmatGraph.gexf")
    pw.write(toGexf(util.GraphGenerators.rmatGraph(sc, 32, 60)))
    pw.close()
  }

  def toGexf[VD,ED](g:Graph[VD,ED]) =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "    <nodes>\n" +
      g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      "    </nodes>\n" +
      "    <edges>\n" +
      g.edges.map(e => "      <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      "    </edges>\n" +
      "  </graph>\n" +
      "</gexf>"


  /**
    *
    * @param sc
    */
  def personalPageRank(sc :SparkContext): Unit ={
    val g = GraphLoader.edgeListFile(sc, "file/cit-HepTh.txt")
    val c = g.pageRank(9207016, 0.001)
      .vertices
      .filter(_._1 != 9207016)
      .reduce((a,b) => if(a._2 > b._2) a else b)
    System.out.println(c)
  }

  /**
    *
    * @param sc
    */
  def triangleSlashDot(sc : SparkContext): Unit ={
    val g = GraphLoader.edgeListFile(sc, "file/soc-Slashdot0811.txt")

    val g2 = Graph(g.vertices, g.edges.map(e => if(e.srcId < e.dstId) e else new Edge(e.dstId, e.srcId, e.attr)))
      .partitionBy(PartitionStrategy.RandomVertexCut)

    (0 to 6).map(i => g2.subgraph(vpred = (vid, _) => vid >= i*10000 && vid < (i+1)*10000)
      .triangleCount().vertices.map(_._2).reduce(_ + _)).foreach( a => System.out.println(a))
  }

  def connectComponents(sc :SparkContext): Unit ={
    val g = Graph(sc.makeRDD((1L to 7L).map((_, ""))), sc.makeRDD(Array(Edge(2L, 5L, ""), Edge(5L, 3L, ""), Edge(3L, 2L, ""),
      Edge(4L, 5L, ""), Edge(6L, 7L, "")
    ))).cache()

    g.connectedComponents().vertices.map(_.swap).groupByKey().map(_._2).collect()
  }


}
