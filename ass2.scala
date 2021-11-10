val t1 = System.nanoTime
//read the textfile
val rdd = sc.textFile("ass2-eda-18.txt")

//collect the dataset
val ds = rdd.collect.toArray

//import required libraries
import math._
import java.io._

//start the printwriter
var pw = new PrintWriter(new File("ass2_out.txt" ))

//required global variables
var x:Int = 0
var L = ds.length-1

//iterate for every vector
for (x <- 0 to L)
{
//define the distance function
def dist(z: Array[Double], y: Array[Double]):Double = {

//required local variables
var l = z.length-1
var i:Int=0
var k:Double=0

//iterate for every feature
for (i <- 0 to l-1)
{ 
var j=pow((z(i)-y(i)),2)
k+=j
} 

//calculate the distance measure
k=sqrt(k)
return k
}

//test Data Point
var  test = ds(x).split(" ").map(_.toDouble)

//training Data Set
var train = ds.filter(!_.contains(ds(x))).map(b => b.split(" ").map(_.toDouble))

//map neighbours
var neighbours=train.map(instance => (dist(instance,test),(instance(instance.length-1)).toInt))

//find 50 nearest neighbours
var NN = neighbours.toSeq.sortWith(_._1 < _._1).take(50)

//take out the classes
var (dis,cls) = NN.unzip

//map the classes
var clsmap = sc.parallelize(cls.map(s=>(s,1)))

//reduce to find class density
var out = clsmap.reduceByKey{(a,b)=>(a+b)}

//take out the class density
var (clss, portion) = out.collect.unzip

//find the class proportion
var proportion = portion.map(e => (e.toDouble/50))

//zip class label and its proportion
var fin = clss.zip(proportion)

//make a sorted rdd of class label and proportion
var finl = sc.parallelize(fin.toSeq.sortWith(_._1 < _._1))

//write the rdd to output file
pw.write((x+1).toString + "." + finl.collect.toArray.mkString+"\n")
}

//stop the file writer
pw.close

val duration = (System.nanoTime - t1) / (1e9d*60)
