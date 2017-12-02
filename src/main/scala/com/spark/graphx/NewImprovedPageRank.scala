package com.spark.graphx

import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, GraphLoader}

/**
  *
  */
object NewImprovedPageRank {

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
//      1.0 / EdgeTriplet.srcAttr
      EdgeType(1.0 / EdgeTriplet.attr, 1.0 / EdgeTriplet.attr)
    }

    //重新把顶点数据设置为一
    val inputGraph = weightedEdgesGraph.mapVertices((id, vData) =>
      if(id < 4 ){
        new NodeType(1.0, 1.0, false)
      }
      else{
        new NodeType(1.0, 1.0, true)
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

    val updateVertex = (vId: Long, vData: NodeType, msgSum: Double) =>{
      if(vData.isNew == false){
        if(math.abs(msgSum) > 0.01) {
          new NodeType(vData.curData, vData.curData + msgSum, false)
        }
        else {
          new NodeType(vData.curData, vData.curData, false)
        }
      }
      else {
        if(math.abs(0.15 + 0.85 * msgSum - vData.curData) > 0.01){
          new NodeType(vData.curData, 0.15 + 0.85 * msgSum, true)
        }
        else {
          new NodeType(0.15 + 0.85 * msgSum, 0.15 + 0.85 * msgSum, true)
        }
      }
    }
    //triplet.srcAttr = 1
    val sendMsg = (triplet: EdgeTriplet[NodeType, EdgeType]) => {

      if(triplet.dstAttr.isNew == false)
        {
          if(triplet.attr.prevWeight == -1.0){
            Iterator((triplet.dstId, triplet.srcAttr.curData * triplet.attr.curWeight))
          }
          else if(triplet.attr.prevWeight != triplet.attr.curWeight){
            Iterator((triplet.dstId, triplet.srcAttr.curData * (triplet.attr.curWeight - triplet.attr.prevWeight)))
          }
          else if(math.abs(triplet.srcAttr.prevData - triplet.srcAttr.curData) > 0.01){
            Iterator((triplet.dstId, (triplet.srcAttr.curData - triplet.srcAttr.prevData) * triplet.attr.curWeight))
          }
          else
            Iterator.empty
        }
      else if(math.abs(triplet.srcAttr.prevData - triplet.srcAttr.curData) > 0.01){
        Iterator((triplet.dstId, triplet.srcAttr.curData * triplet.attr.curWeight))
      }
      else
        Iterator.empty

    }

    val aggregateMsgs = (x: Double, y: Double) => x + y

    val pw = new PrintWriter("src/main/resources/NewImprovedPageRankLog.txt")

    val influenceGraph = pregel0[NodeType, EdgeType, Double](pw, inputGraph, firstMessage, iterations, edgeDirection)(updateVertex, sendMsg, aggregateMsgs)

    influenceGraph.vertices.foreach(a => println(a))

  }

  case class NodeType(prevData: Double, curData: Double, isNew: Boolean)

  case class EdgeType(prevWeight: Double, curWeight: Double)
}
