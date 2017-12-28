package com.spark.graphx

import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader}

import scala.reflect.ClassTag

/**
  *
  */
object MergeGraph {

  def apply(graph1:Graph[Double, Int], graph2:Graph[Double, Int]): Graph[Double, Int] = {

    val v = graph1.vertices.map(_._2).union(graph2.vertices.map(_._2)).distinct().zipWithIndex()

    def edgesWithNewVeryexIds(g:Graph[Double, Int]) =
      g.triplets
      .map(et => (et.srcAttr, (et.attr, et.dstAttr)))
      .join(v)
      .map(x => (x._2._1._2, (x._2._2, x._2._1._1)))
      .join(v)
      .map(x => new Edge(x._2._1._1, x._2._2, x._2._1._2))

    Graph(v.map(_.swap), edgesWithNewVeryexIds(graph1).union(edgesWithNewVeryexIds(graph2)))

  }

}
