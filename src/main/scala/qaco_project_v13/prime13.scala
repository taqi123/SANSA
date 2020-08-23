package qaco_project_v13
import org.apache.spark._
import scala.collection.mutable.ListBuffer
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.math._
import scala.io.Source
import scala.util.control.Breaks._
import Array._ 

import java.io.File
import java.io.PrintWriter


case class page(source: Int, dest: Array[Double], existing: Array[Int])
  
object prime13 {
  def main(args: Array[String])
  {
      val sparkConf = new SparkConf().setAppName("qaco_project_v13").setMaster("local[*]")
      val sc = new SparkContext(sparkConf)
      val lines = Source.fromFile("_data/musae_chameleon_edges.csv").getLines().toList.map(x => x.split(','))
      
      var probeE = new ListBuffer[String]
      val rand1 = new Random
      
      val file_obj1 = new File("_data/probe_spark.txt")
      val printw1 = new PrintWriter(file_obj1)
      for(e <- 0 to 5000)
      {
        val probe = 0 + rand1.nextInt((2276-0)+1)
        val k = lines.apply(probe).reduce((a,b) => (a+ "-" +b))
        probeE += k
        printw1.write(k + '\n')
      }
      //probeE.foreach(println)
      printw1.close()
      
      val edj : RDD[Edge[(Int)]] = sc.parallelize(lines).map(x => Edge(x(0).toLong,x(1).toLong,(1)) )
                                                        
      val defaultUser = ("gg","bb")
      val graph = Graph.fromEdges(edj, defaultUser)
      
      val getting_neighbours = graph.collectNeighborIds(EdgeDirection.Out)
      val neighbours = getting_neighbours.map{x => val a = x._1.toInt
                                                   val b = x._2.map(y => y.toInt)
                                                   (a,b)}.collect().sortBy(x => x._1)
                                                   //neighbours.foreach(println)
                                                   
      val degree = graph.outDegrees.collect().sortBy(x => x._1)

      var p = -1
      var LiPh = Array.ofDim[Double](2277,2277).map{z => val lamda = 3
                                                         val ebsilon = 0.1
                                                         p = p + 1
                                                         var j = 0
                                                         var k = 0.0
                                                         z.map{y => if(neighbours(p)._2.contains(j))
                                                                    {
                                                                      k =  lamda * (1d + ebsilon) 
                                                                      //print("yup")
                                                                    }
                                                                    else
                                                                    {
                                                                       k = lamda * ebsilon
                                                                    }
                                                                j = j+1
                                                                k
                                                              }
                                                    }
      /*LiPh.foreach(x => x.foreach{y => if(y > 1)
                                       {println(y)}
                                 }
                  )
                  *                                              
                  */

      val making_adjM = getting_neighbours.mapPartitions{ x => val neig = neighbours             
                                                               val deg = degree                  
                                                               var pathe: List[Array[Int]] = List()
                                                               var adjm: List[page] = List()
                                                               while(x.hasNext)
                                                               {
                                                                 val a = x.next()
                                                                 var row = Array.ofDim[Double](2277)
                                                                 for(i<-0 to 2276)
                                                                 {
                                                                   var eita = 0.0
                                                                   if(a._1.toInt != i){
                                                                   val d = concat(neig(a._1.toInt)._2,neig(i)._2).sortBy(y => y)
                                                                   for(j<- 0 to d.length-2)
                                                                   {
                                                                     if(d(j)==d(j+1))
                                                                     {
                                                                       eita = eita + 1d/deg(d(j))._2
                                                                     }
                                                                   }
                                                                   eita = eita + 2 * 1d/(deg(i)._2 * deg(a._1.toInt)._2)
                                                                   }
                                                                   row(i) = eita
                                                                 }
                                                                 row(a._1.toInt) = 0.0
                                                                 adjm = adjm :+ page(a._1.toInt, row, neig(a._1.toInt)._2)
                                                               }
                                                               
                                                               var QP = Array.ofDim[Double](2277).map(z => 0.7071067)
                                                               var Nc = 0
                                                               while(Nc < 500)
                                                               {
                                                                 var path = Array.ofDim[Int](20).map(y => -1)
                                                                 val rand = new Random()
                                                                 val g = 0 + rand.nextInt((100-0)+1)
                                                                 var nex = adjm.apply(g)
                                                                 path(0) = nex.source
                                                                 breakable{
                                                                 for(i<- 1 to 19)       
                                                                 {
                                                                   var maxp = 0.0
                                                                   var next = -1
                                                                   var LP = LiPh(nex.source)
                                                                   for(j <- 0 to nex.dest.length-1)
                                                                   {
                                                                    // if(nex.existing.contains(j))
                                                                    // {
                                                                    //   LP(j) = lamda * (1 + ebsilon)
                                                                       //println(LP(j))
                                                                    // }
                                                                    // else if(nex.source == j)
                                                                     //{
                                                                      // LP(j) = 0.0
                                                                     //}
                                                                     val prob = nex.dest(j) * LP(j) * QP(j)
                                                                     if(prob > maxp && !path.contains(j) && adjm.exists(p => p.source == j))
                                                                     {
                                                                       maxp = prob
                                                                       next = j
                                                                     }
                                                                   }
                                                                   if(next != -1)
                                                                   {
                                                                     path(i) = next
                                                                     val iter = adjm.iterator
                                                                     while(iter.hasNext)
                                                                     {
                                                                       val k = iter.next()
                                                                       if(k.source == next)
                                                                       {
                                                                         nex = k
                                                                       }
                                                                     }
                                                                   }
                                                                   else
                                                                     break
                                                                 }}
                                                                 pathe = pathe :+ path

                                                                 
                                                                 path.foreach{x => if(x != -1)
                                                                                   {QP(x) = QP_update(QP(x))}
                                                                             }
                                                                 Nc = Nc+1
                                                               }
                                                               def QP_update(qp: Double): Double = 
                                                               {
                                                                  var theta = 0.0
                                                                  var alpha = 0.7071067
                                                                  var beta =  sqrt(1 - pow(alpha,2)) 
                                 
                                                                  val delta_theta = 0.04 * 3.142857
                                 
                                                                  if((alpha*beta)>0)
                                                                  {
                                                                     theta = delta_theta * 1
                                                                  }
                                                                  else if((alpha*beta)<0)
                                                                  {
                                                                     theta = delta_theta * -1
                                                                  }
                                                                  else if((alpha*beta)==0)
                                                                  {
                                                                     theta = 0
                                                                  }
                                 
                                                                  alpha = (cos(theta) * alpha) - (sin(theta) * beta)
                                                                  beta  = (sin(theta) * alpha) + (cos(theta) * beta)
                                 
                                                                  var qp2 = 0.0
                                                                  if(alpha>=beta)
                                                                  {
                                                                     qp2 = alpha
                                                                  }
                                                                  else
                                                                  {
                                                                     qp2 = beta
                                                                  }
                                 
                                                                  (1d/pow(qp2,2))
                                                                  }
                                                               val it = pathe.iterator
                                                           it 
                                                           }.collect()
                                                   
                                         
      making_adjM.foreach{x => for(j<- 0  to x.length-2)
                                {if(x(j) != -1 && x(j+1) != -1)
                                  {LiPh(x(j))(x(j+1)) = LiPh(x(j))(x(j+1)) + 1
                                  //print(LiPh(x(j))(x(j+1)) + " " )
                                  }
                                }
      //println
      }

         making_adjM.foreach{x => x}

      
      val file_obj2 = new File("_data/result_spark.txt")
      val printw2 = new PrintWriter(file_obj2)
      
    for(i <- 0 to 2276)
    {
      for(j <- 0 to 2276)
      {
        if(LiPh(i)(j) >= 4)
        {
          printw2.write(i + "-" + j + "-" + LiPh(i)(j) + '\n')
        }
      }
    }
      
    printw2.close()  
      
  } 
  
}

