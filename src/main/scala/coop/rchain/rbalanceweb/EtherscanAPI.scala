package coop.rchain.rbalance.txns
//import scala.concurrent.ExecutionContext.global
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent._
import cats.effect._
import io.circe._
//import io.circe.literal._
import org.http4s._
import org.http4s.dsl.io._
import cats.effect._
import org.http4s.circe._
import org.http4s.implicits._
import org.http4s.client.blaze._
import org.http4s.client._

object EtherscanAPIAccess {
  import AdjustmentConstants._
  implicit val cs    : ContextShift[IO] = IO.contextShift(global)
  implicit val timer : Timer[IO]        = IO.timer(global)

  BlazeClientBuilder[IO](global).resource.use { client =>
    // use `client` here and return an `IO`.
    // the client will be acquired and shut down
    // automatically each time the `IO` is run.
    IO.unit
  }

  val blockingEC                          = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(5))
  val httpClient       : Client[IO]       = JavaNetClientBuilder[IO](blockingEC).create
  val apiKey           : String           = "251USXDI6XCV4CQYA6UCQ6Y5JPBR7FPXAC"
  val apiKey2          : String           = "AHMYX9PI91G4Q6PT772QY31M3668HJTNDR"

  def etherscanTxnArray( addr : String ) : Vector[Json] = {
    val lowerCaseAddr = addr.toLowerCase()
    val etherscanURI =
      s"http://api.etherscan.io/api?module=account&action=tokentx&address=$lowerCaseAddr&startblock=$minBlockHeight&endblock=$maxBlockHeight&sort=asc&apikey=$apiKey"
    val etherscanDataStr = httpClient.expect[String]( etherscanURI ).unsafeRunSync
    val etherscanJson = Ok( etherscanDataStr ).flatMap( _.as[Json] ).unsafeRunSync
    val etherscanTxnRslt = etherscanJson \\ "result"
    etherscanTxnRslt( 0 ).asArray.getOrElse( throw new Exception( "not an array" ) )
  }

  def txnRecordData( addr : String, e : Json ) : Option[( String, String, Double, String, String )] = {
    val trgtAddr = ( e \\ "to" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
    val blockNumber = ( e \\ "blockNumber" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) ).toInt
    if ( ( trgtAddr != addr )  && ( blockNumber <= maxBlockHeight ) ){
      val srcAddr = ( e \\ "from" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
      val amt = ( e \\ "value" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) ).toDouble
      val hash = ( e \\ "hash" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
      val blockHash = ( e \\ "blockHash" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
      Some( ( srcAddr, trgtAddr, amt, hash, blockHash ) )
    }
    else {
      None
    }
  }

  def getBalanceDataFromEtherscan( addr : String ) = {
    // Etherscan is not accepting this action!
    val etherscanURI =
      s"https://api.etherscan.io/api?module=account&action=tokenbalance&contractaddress=$rhocContractAddr=$addr&tag=latest&apikey=$apiKey"
    val etherscanDataStr = httpClient.expect[String]( etherscanURI ).unsafeRunSync
    val etherscanJson = Ok( etherscanDataStr ).flatMap( _.as[Json] ).unsafeRunSync
    val etherscanTxnRslt = etherscanJson \\ "result"
    //etherscanTxnRslt( 0 ).asArray.getOrElse( throw new Exception( "not an array" ) )
    etherscanTxnRslt
  }

}
