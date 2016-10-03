//object PopulatonClusters{
//    def main(args: Array[String]){
//        val panelfile = "integrated_call_samples_v3.20130502.ALL.panel"
//        import scala.io.Source
////        val biggroups = Source.fromFile(panelfile).getLines().drop(1).map(line => {
////            val tokens = line.split(",")
////            (tokens(1), 1)
////        }).toMap.countByValue()
//
//        val biggroups = sc.textFile(panelfile).map(_.split(" ")).map(x => (x(1), 1)).countByValue()
//    }
//}

val panelfile = "integrated_call_samples_v3.20130502.ALL.panel"
import scala.io.Source
//        val biggroups = Source.fromFile(panelfile).getLines().drop(1).map(line => {
//            val tokens = line.split(",")
//            (tokens(1), 1)
//        }).toMap.countByValue()

val tf = sc.textFile(panelfile)

// Part 2
val biggroups = tf.map(_.split("\t")).map(_(1)).countByValue().filter(_._2 >= 90).keys

// Part 3
val people = tf.map(_.split("\t")).map(x => (x(0), x(1))).filter(x => biggroups.toList.contains(x._2)).map(x => x._1).
  collect().toSet

println("\nPopulations with more than 90 individuals: "+biggroups.size)
println("Individuals from these populations: "+people.size)

