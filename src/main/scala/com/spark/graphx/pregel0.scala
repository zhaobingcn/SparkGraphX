package com.spark.graphx

import java.io.PrintWriter

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  *
  */
object pregel0 extends Logging {
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (pw: PrintWriter,
   graph: Graph[VD, ED],
   initialMsg: A,
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
   sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
   mergeMsg: (A, A) => A)
  : Graph[VD, ED] = {
    //要求最大迭代数大于0，不然报错。
    require(maxIterations > 0, s"Maximum number of iterations must be greater than 0," +
      s" but got ${maxIterations}")
    //第一次迭代，对每个节点用vprog函数计算。
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()

    // 根据发送、聚合信息的函数计算下次迭代用的信息。
    var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
    //数一下还有多少节点活跃
    var activeMessages = messages.count()
    // 下面进入循环迭代
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      pw.write(activeMessages + "\n")
      // 接受消息并更新节点信息
      prevG = g
      g = g.joinVertices(messages)(vprog).cache()

      val oldMessages = messages
      // Send new messages, skipping edges where neither side received a message. We must cache
      // messages so it can be materialized on the next line, allowing us to uncache the previous
      /*iteration这里用mapReduceTriplets实现消息的发送和聚合。mapReduceTriplets的*参数中有一个map方法和一个reduce方法，这里的*sendMsg就是map方法，*mergeMsg就是reduce方法
      */
      messages = g.mapReduceTriplets(
        sendMsg, mergeMsg, Some((oldMessages, activeDirection))).cache()
      // The call to count() materializes `messages` and the vertices of `g`. This hides oldMessages
      // (depended on by the vertices of g) and the vertices of prevG (depended on by oldMessages
      // and the vertices of g).
      activeMessages = messages.count()

      logInfo("Pregel finished iteration" + activeMessages)

      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      // count the iteration
      i += 1
    }
    messages.unpersist(blocking = false)
    pw.close()
    g
  }
}
