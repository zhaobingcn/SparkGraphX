package com.spark.graphx

import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  *
  */
object ImprovedShortestPath {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("pregel")

    val sc = new SparkContext(conf)

    //    val graph = GraphLoader.edgeListFile(sc, "src/main/resources/test_web.txt")
    //    val sourceId: VertexId = 0
    //    val g = graph.mapVertices((id, _) => if(id == sourceId) 0.0 else Double.PositiveInfinity)
    //    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

    //    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
    //      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
    //      triplet => {
    //        // Send Message
    //        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
    //          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    //        } else {
    //          Iterator.empty
    //        }
    //      },
    //      (a, b) => math.min(a, b) // Merge Message
    //    )


    //    sssp.vertices.take(10).foreach(a => println(a))

    //以Google的网页链接文件(后面由下载地址)为例，演示pregel方法，找出从v0网站出发，得到经过的步数最少的链接网站，类似于附近地图最短路径算法
    val graph = GraphLoader.edgeListFile(sc, "src/main/resources/test_web.txt")

    graph.vertices.foreach(a => println(a))

    val sourceId: VertexId = 0;
    //定义源网页Id
    val g: Graph[NodeType, Double] = graph.mapVertices((id, attr) => if (id == 0) new NodeType(0.0, true) else new NodeType(Double.PositiveInfinity, true)).mapEdges(a => a.attr.toDouble)
    //pregel底层调用GraphOps的mapReduceTriplets方法，一会儿解释源代码

    val pw = new PrintWriter("src/main/resources/improvedShortestPath.txt")

    val result = pregel[Double, NodeType, Double](pw, g, Double.PositiveInfinity)(
      (id, vd, newVd) => new NodeType(math.min(vd.data, newVd), true), //这个方法的作用是更新节点VertexId的属性值为新值，以利于innerJoin操作
      triplets => {
        //map函数
        if (triplets.srcAttr.data + triplets.attr < triplets.dstAttr.data) {

          Iterator((triplets.dstId, triplets.srcAttr.data + triplets.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) //reduce函数
    )
    //输出结果，注意pregel返回的是更新VertexId属性的graph，而不是VertexRDD[(VertexId,VD)]
    result.vertices.take(10).foreach(a => println(a))//注意过滤掉源节点
    //找出路径最短的点



  }

  def min(a:(VertexId,Double),b:(VertexId,Double)):(VertexId,Double) = {
    if(a._2 < b._2) a else b
  }

  def pregel[A:ClassTag,VD:ClassTag,ED:ClassTag]( pw:PrintWriter, graph:Graph[VD,ED],initialMsg:A,maxInterations:Int = Int.MaxValue,activeDirection:EdgeDirection =  EdgeDirection.Either)(
    vprog:(VertexId,VD,A) => VD,
    sendMsg:EdgeTriplet[VD,ED] =>Iterator[(VertexId,A)],
    mergeMsg:(A,A) => A)
  : Graph[VD,ED] = {
    pregel0(pw, graph,initialMsg,maxInterations,activeDirection)(vprog,sendMsg,mergeMsg)//调用apply方法
  }

  case class NodeType(data:Double, isNew:Boolean)



}
