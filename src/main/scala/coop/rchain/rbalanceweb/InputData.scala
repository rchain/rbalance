package coop.rchain.rbalance.txns
import scala.collection.immutable.Set
import scala.collection.immutable.HashSet
import scala.collection.immutable.Map
import scala.collection.immutable.HashMap

trait InputCSVData {
  val inputSource : String = "RHOC-tx_7598478-9371757.csv"
  val inputDir : String = "src/main/resources"

  def loadData( source : String, dir : String ) : List[Array[String]] = {
    val bufferedSource = scala.io.Source.fromFile( s"$dir/$source" )
    val line :: lines = bufferedSource.getLines.toList
    val rslt = lines.map( _.split( "," ).map( _.trim ) )
    bufferedSource.close
    rslt
  }
}
