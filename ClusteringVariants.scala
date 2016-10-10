import org.bdgenomics.adam.rdd.ADAMContext._
import collection.JavaConverters._
import util.Random.nextInt
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector


def convertAlleles(x: java.util.List[org.bdgenomics.formats.avro.GenotypeAllele] ) = { x.asScala.map(_.toString).count(_ != "Ref").asInstanceOf[Double] }

val panelfile = "integrated_call_samples_v3.20130502.ALL.panel"

val tf = sc.textFile(panelfile)

// Part 2
val biggroups = tf.map(_.split("\t")).map(_(1)).countByValue().filter(_._2 >= 90).keys

// Part 3
val people = tf.map(_.split("\t")).map(x => (x(0), x(1))).filter(x => biggroups.toList.contains(x._2)).map(x => x._1).
  collect().toSet

println("\nPopulations with more than 90 individuals: "+biggroups.size)
println("Individuals from these populations: "+people.size)
println("--------------------------------------------------------------------------------")

println("\nPart 3")

val data = sc.loadGenotypes("small.adam").rdd.map(r => (r.contigName, r.start, r.end, r.sampleId, convertAlleles(r.alleles))).filter(x => people.contains(x._4))

val totalVariants = data.map(x => ((x._1, x._2, x._3), 1L)).reduceByKey(_ + _)
val uniqueVariants = totalVariants.filter(_._2 <= people.size).map(_._1).zipWithIndex().collectAsMap
val variantSize = uniqueVariants.size.asInstanceOf[Int]

println("\nTotal variants: " + totalVariants.count())
println("Variants with right number of samples: " + variantSize)

val userVectors = data.filter(x => uniqueVariants.contains((x._1, x._2, x._3))).map(x => (x._4, (uniqueVariants((x._1, x._2, x._3)) ,x._5))).groupBy(x => x._1).map(x => (x._1, Vectors.dense(x._2.map(z => z._2).toArray.sortWith(_._1 < _._1).map(_._2))))

val k = biggroups.size // Populations over 21


// Pick k random vectors from userVectors
val centroids = userVectors.takeSample(false, k, System.nanoTime.toInt)
//
// val MAX = 100 //arbitrary amount
//
// // val sD = Vectors.sqdist(centroids(0), userVectors.first()._2)
//
// for( i <- 0 until 100) {
//   val minD = Array.fill(people.size)((0, Double.MaxValue))
//   for(centroid <- 0 until k) {
//     for(user <- 0 until people.size) {
//       val sD = Vectors.sqdist(centroids(centroid), userVectors(user)._2)
//       if(sD < minD(user)._2) {
//         minD(user) = (k, sD)
//       }
//     }
//   }
// }
