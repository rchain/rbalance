package coop.rchain.rbalance.transitive
import scala.collection.immutable.Set
import scala.collection.immutable.HashSet

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

trait Closure[Src] {  
  def computeClosure( src : Src, acc : Set[Src], next : Src => Set[Src] ) : Set[Src] = {
    next( src ).flatMap( ( x ) =>{ computeClosure( x, acc, next ) } ) + src
  }
  def next : Src => Set[Src]
  def close( src : Src ) : Set[Src] = computeClosure( src, new HashSet[Src](), next )
}

trait RHOCTxn {
  def src : String
  def trgt : String
  def amt : Float
  def hash : String
  def blockHash : String
  def justification : Set[RHOCTxn]
  override def toString() : String = {
    s"$src -$amt-> $trgt"
  }
}
class InitialRHOCTxn( override val trgt : String ) extends RHOCTxn {
  override def src : String = ""
  override def amt : Float = 0
  override def hash : String = ""
  override def blockHash : String = ""
  override def justification : Set[RHOCTxn] = new HashSet[RHOCTxn]()
}
case class RHOCTxnRep( 
  src : String, trgt : String, 
  amt : Float, 
  hash : String, blockHash : String, 
  justification : Set[RHOCTxn] 
) extends RHOCTxn

object RHOCTxnClosure extends Closure[RHOCTxn] {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  BlazeClientBuilder[IO](global).resource.use { client =>
    // use `client` here and return an `IO`.
    // the client will be acquired and shut down
    // automatically each time the `IO` is run.
    IO.unit
  }

  val blockingEC = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(5))
  val httpClient: Client[IO] = JavaNetClientBuilder[IO](blockingEC).create
  val apiKey : String = "251USXDI6XCV4CQYA6UCQ6Y5JPBR7FPXAC"
  val blockHeight : Int = 9371743

  def nextTxns( txn : RHOCTxn ) : Set[RHOCTxn] = {
    val lowerCaseAddr = txn.trgt.toLowerCase()
    val etherscanURI =
      s"http://api.etherscan.io/api?module=account&action=tokentx&address=$lowerCaseAddr&startblock=0&endblock=999999999&sort=asc&apikey=$apiKey"
    val etherscanDataStr = httpClient.expect[String]( etherscanURI ).unsafeRunSync
    val etherscanJson = Ok( etherscanDataStr ).flatMap( _.as[Json] ).unsafeRunSync
    val etherscanTxnRslt = etherscanJson \\ "result"
    val etherscanTxnArray = etherscanTxnRslt( 0 ).asArray.getOrElse( throw new Exception( "not an array" ) )
    val rslt = etherscanTxnArray.foldLeft( new HashSet[RHOCTxn]() )(
      { ( acc, e ) => {
        val trgtAddr = ( e \\ "to" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
        val blockNumber = ( e \\ "blockNumber" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) ).toInt
        if ( ( trgtAddr != lowerCaseAddr )  && ( blockNumber <= blockHeight ) ){
          val srcAddr = ( e \\ "from" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
          val amt = ( e \\ "value" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) ).toFloat
          val hash = ( e \\ "hash" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
          val blockHash = ( e \\ "blockHash" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )          
          acc + new RHOCTxnRep( srcAddr, trgtAddr, amt, hash, blockHash, new HashSet[RHOCTxn]() + txn )
        }
        else {
          acc
        }
      } }
    )
    println( "Next generation:" )
    rslt.map( { txn => println( txn ) } )
    rslt
  }

  override def next = { ( x : RHOCTxn ) => nextTxns( x ) }

  def closeAddr( addr : String ) = close( new InitialRHOCTxn( addr ) )
}




