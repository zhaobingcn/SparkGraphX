package com.spark.graphx

import java.io.PrintWriter

import com.spark.graphx.ImprovedShortestPath.NodeType
import com.spark.graphx.NewImprovedPageRank.EdgeType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, GraphLoader, VertexId}

/**
  * C
  */
object ConnectComponents {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("pregel")

    val sc = new SparkContext(conf)

    val input1 = GraphLoader.edgeListFile(sc, "src/main/resources/test_web.txt")

    //**********************计算顶点的出度数A,并更新edge的值为1/A以及更新顶点为1

    val graph1 = input1.mapVertices((id, data) => id.toDouble)

    val input2 = GraphLoader.edgeListFile(sc, "src/main/resources/merge_web.txt")

    val graph2 = input2.mapVertices((id, data) => id.toDouble)

    val graph = MergeGraph(graph1, graph2)


    graph.triplets.foreach(a => println(a))

//    secondGraph.vertices.foreach(a => println(a))



    val outDegrees = graph.outDegrees

    val outDegreesGraph = graph.outerJoinVertices(outDegrees) {
      (vId, vData, OptOutDegree) =>
        //更新顶点数据为出度
        OptOutDegree.getOrElse(0)
    }


    val weightedEdgesGraph = outDegreesGraph.mapTriplets { EdgeTriplet =>
      //边界的值
      //1/出度数目
      //      1.0 / EdgeTriplet.srcAttr
      EdgeType(1.0 / EdgeTriplet.attr, 1.0 / EdgeTriplet.attr)
    }



    //重新把顶点数据设置为一
    val inputGraph = weightedEdgesGraph.mapVertices((id, vData) =>
      if(id < 4 ){
        new NodeType(1.0, false)
      }
      else{
        new NodeType(1.0, true)
      }

    )

    val secondInputGraph = weightedEdgesGraph.mapVertices((id, vData) => 6)


  }
}
