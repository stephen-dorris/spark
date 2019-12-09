package PR


import java.io.{File, FileWriter}


import org.apache.log4j.{FileAppender, Level, PatternLayout}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object PageRank {


  def createSyntheticGraph(k: Int): List[(Int, List[Int])] = {
    var edges: List[(Int, List[Int])] = List.empty
    for (i <- 1 to k * k) {
      if (i % k == 0) {
        edges = edges.++(List((i, List(0))))
      }
      else {
        edges = edges.++(List((i, List(i + 1))))
      }
    }
    edges
  }


  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = org.apache.log4j.Logger.getRootLogger
    val fileAppender = new FileAppender()
    fileAppender.setFile("/Users/stephendorris/LocalDocuments/FALL2019/LargeScale/sdorris-Assignment-4/PRSpark/outputPageRank/logs.txt");
    fileAppender.setLayout(new PatternLayout)
    fileAppender.setAppend(false)
    fileAppender.setThreshold(Level.INFO)
    fileAppender.activateOptions()
    logger.addAppender(fileAppender)
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("PageRank")
    val sc = new SparkContext(conf)
    val k = args(0).toInt

    // Create Synthetic Graph as described in README under sect "Synthetic Graph Specs"
    val graph_lst = createSyntheticGraph(k)


    // Broadcast Vars
    val initRank = sc.broadcast(1.0 / (k * k)) // initial rank for non dummy verts
    val graphSize = sc.broadcast(graph_lst.size)
    val alpha = sc.broadcast(.85)


    //Create Static Graph RDD
    val graphRDD = sc.parallelize(graph_lst, 2).persist()


    val dummyRank = List[(Int, Double)] {
      (0, 0.toDouble)
    }

    // Create Non Static Ranks RDD
    var ranks_lst = dummyRank.++(graph_lst.map({case (x, _) => (x, initRank.value) }))
    var ranksRDD = sc.parallelize(ranks_lst, graphRDD.getNumPartitions).persist()
    //      .partitionBy(graphRDD.partitioner match {
    //      case Some(p) => p
    //      case _ => new HashPartitioner(graphRDD.partitions.length)
    //
    //
    //    })// Same Partitioner as GraphRDD?


    for (_ <- 1 to 10) {
      val contribs = graphRDD.join(ranksRDD).flatMap({ case (_, (outlinks, pageRank: Double)) =>
        outlinks.map(link => (link, pageRank / outlinks.size.toDouble))
      }).reduceByKey(_ + _)

      var danglingMass = contribs.lookup(0) match {
        case seq: Seq[Double] => seq.head
        case _ => throw new Exception("Dangling Mass Does Not Exist...")
      }

      ranksRDD = graphRDD.leftOuterJoin(contribs).map({
        case (page, (_, maybeContribution)) =>
          val prob_randomHop = (1.toDouble - alpha.value) * initRank.value
          val distrib_danglingMass = alpha.value * (danglingMass / graphSize.value.toDouble)
          maybeContribution match {
            case Some(contribution) =>
              val newRank = prob_randomHop + distrib_danglingMass + (alpha.value * contribution)
              (page, newRank)
            case _ =>
              val newRank = prob_randomHop + distrib_danglingMass
              (page, newRank)
          }
      })
    }
     logger.info(ranksRDD.toDebugString)


    // Efficient ?
       var x = ranksRDD.collect().sortBy(_._1)

        val sb = new StringBuilder

        x.foreach(pair => sb ++= pair.toString()+"\n")
        import java.io.PrintWriter
        val pw = new PrintWriter(new File("PRSpark/outputPageRank/out.txt")).print(sb.toString())


  }


}