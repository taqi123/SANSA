package qaco_project_v12
import org.apache.spark._
import scala.collection.mutable.ListBuffer
//import org.apache.spark.graphx.lib._
//import org.apache.spark.graphx._
//import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.math._
import scala.io.Source
import scala.util.control.Breaks._
import Array._

import java.io.File
import java.io.PrintWriter


object prime12 {
  def main(args: Array[String])
  {
      val sparkConf = new SparkConf().setAppName("qaco_project_v11").setMaster("local[*]")
      val sc = new SparkContext(sparkConf)
      val line = Source.fromFile("_data/musae_chameleon_edges.csv").getLines().toList.map{ x=> val a = x.split(',')
                                                                                          (a(0).toInt,a(1).toInt)}
      var AdjM = Array.ofDim[Int](2277,2277)
      val line_it = line.iterator
      var AdjM_e = Array.ofDim[Double](2277,2277)
      var LP = Array.ofDim[Double](2277,2277).map(x => x.map(y => 1.0))
      var QP = Array.ofDim[Double](2277).map(x => 2.0)
      val lamda = 3
      val ebsilon = 0.1
      var probeE = new ListBuffer[(Int,Int)]
      val rand = new Random

      val file_obj1 = new File("_data/probe_serial.txt")
      val print_w1 = new PrintWriter(file_obj1)
      
      for(e <- 0 to 5000)
      {
        val probe = 0 + rand.nextInt((2276-0)+1)
        //probeE += line.apply(probe)
        val k = line.apply(probe)
        print_w1.write(k._1 + "-" + k._2 + '\n')
        //print_w1.write(line.apply(probe)._1)
        //print(line.apply(probe)._1)
        //print_w1.write("-")
        //print("-")
        //print_w1.write(line.apply(probe)._2)
        //print(line.apply(probe)._2)
        //print_w1.write('\n')
      }
      probeE.foreach(println)
      print_w1.close()
      
      
      while(line_it.hasNext)
      {
        val c = line_it.next()
        AdjM(c._1)(c._2) = 1
      }
      
      var degree = Array.ofDim[Int](2277)
      for(i<- 0 to AdjM.length-1)
      {
        var count = 0
        for(j <- 0 to AdjM.length-1)
        {
          if(AdjM(i)(j)>0)
          {
            count = count+1
          }
        }
        degree(i) = count
      }
      
      for(i<-0 to AdjM.length-1)
      {  
        for(j<-0 to AdjM.length-1)
        {
          if(i == j)
          {
            AdjM_e(i)(j) = 0.0
          }
          else
          { 
            var eita = 0.0
            val c_neighbors = concat(AdjM(i), AdjM(j)).sortBy(x => x)
            for(k<- 0 to c_neighbors.length-1)
            {
              if(c_neighbors(i) == c_neighbors(i+1))
              {
                eita = eita + (1d/degree(c_neighbors(i)))
              }
            }
            eita = eita + 2 * (1d/degree(i) * degree(j))
            
            AdjM_e(i)(j) = eita
            //AdjM_e(j)(i) = eita
          }
          
          if(AdjM(i)(j) == 1)
          {
            LP(i)(j) = lamda * (1d + ebsilon)
          }
          else
          {
            LP(i)(j) = lamda * ebsilon
          }
        }
      }
      
 /*     var count1 = 0
      var count2 = 0
      for(i<- 0 to AdjM_e.length-1)
      {
        for(j<- 0 to AdjM_e.length-1)
        {
          if(LP(i)(j)<=0.0d )
          {
            count1 = count1 + 1
          }
          
          if(AdjM_e(i)(j)<= 0.0d)
          {
             count2 = count2 + 2
          }
        }
      }
      println("count1:" + count1)
      println("count2" + count2)
      * 
      */
      
      val file_obj2 = new File("_data/serial_result.txt")
      val print_w2 = new PrintWriter(file_obj2)
            
      for(Nc<- 1 to 500)
      {
        for(ants<-1 to 4)
        {
          var i = 0+rand.nextInt((2276-0)+1)
          var path = new ListBuffer[Int]
          path += i
          breakable{
          for(path_l <- 0 to 11)
          {
            var max_prob = 0.0
            var curr_i = -1
            for(j<- 0 to AdjM_e.length-1)
            {
              val prob = AdjM_e(i)(j) * LP(i)(j) * QP(j)
              //print(prob + " ")
              if(prob>max_prob && !path.contains(j))
              {
                max_prob = prob
                curr_i = j
              }
            }
            //println
            if(curr_i != -1)
            {
              path += curr_i
              i = curr_i
            }
            else
              break
          }}
          val pathe = path.toArray
          for(i<- 0 to pathe.length-2)
          {
            LP(pathe(i))(pathe(i+1)) = LP(pathe(i))(pathe(i+1)) + 1
            QP(pathe(i+1)) = QP_update(pathe(i+1))
          }
          
        }
      }
      
      for(i <- 0 to LP.length-1)
      {
        for(j<- 0 to LP.length-1)
        {
          if(LP(i)(j) > 1)
          {
            print_w2.write(i + "-" + j + "-" + LP(i)(j) + '\n')
          }
        }
      }
     
      print_w2.close()

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
  
  
}  
