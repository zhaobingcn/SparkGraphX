package com.spark.graphx

import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, GraphLoader}

/**
  *
  */
object NormalPageRank {

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
    val inputGraph = weightedEdgesGraph.mapVertices((id, vData) => new DataType(1.0, 1.0))
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
      if(math.abs(0.15 + 0.85 * msgSum - vData.nowData) > 0.1)
        new DataType(vData.nowData ,0.15 + 0.85 * msgSum)
      else
        new DataType(0.15 + 0.85 * msgSum, 0.15 + 0.85 * msgSum)
    }
    //triplet.srcAttr = 1
    val sendMsg = (triplet: EdgeTriplet[DataType, Double]) => {

      if(math.abs(triplet.srcAttr.prevData - triplet.srcAttr.nowData) > 0.000001){
        Iterator((triplet.dstId, triplet.srcAttr.nowData * triplet.attr))
      }
      else
        Iterator.empty
    }

    val aggregateMsgs = (x: Double, y: Double) => x + y

    val pw = new PrintWriter("src/main/resources/NormalPageRankLog.txt")

    val influenceGraph = pregel0[DataType, Double, Double](pw, inputGraph, firstMessage, iterations, edgeDirection)(updateVertex, sendMsg, aggregateMsgs)

    influenceGraph.vertices.foreach(a => println(a))
  }

  case class DataType(prevData: Double, nowData: Double)
}
