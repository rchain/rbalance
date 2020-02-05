package coop.rchain.rbalance.txns
import scala.collection.immutable.Set
import scala.collection.immutable.HashSet
import scala.collection.immutable.Map
import scala.collection.immutable.HashMap

trait InputCSVData {
  def loadData( source : String, dir : String ) : List[Array[String]] = {
    val bufferedSource = scala.io.Source.fromFile( s"$dir/$source" )
    val line :: lines = bufferedSource.getLines.toList
    val rslt = lines.map( _.split( "," ).map( _.trim ) )
    bufferedSource.close
    rslt
  }

  def loadWalletData() : List[Array[String]] = {
    loadData( AdjustmentConstants.walletSource, AdjustmentConstants.sourceDir )
  }

  def loadTxnData() : List[Array[String]] = {
    loadData( AdjustmentConstants.txnSource, AdjustmentConstants.sourceDir )
  }
}
