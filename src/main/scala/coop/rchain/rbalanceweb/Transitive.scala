package coop.rchain.rbalance.transitive
import scala.collection.immutable.Set
import scala.collection.immutable.HashSet
import scala.collection.immutable.Map
import scala.collection.immutable.HashMap

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

trait Closure[Key,Src] {  
  def computeClosure( src : Src, acc : Map[Key,Set[Src]], next : Src => Set[Src] ) : Map[Key,Set[Src]] = {
    next( src ).foldLeft( acc + ( key( src ) -> next( src ) ) )( {
      ( a, x ) => {
        a.get( key( x ) ) match {
          case Some( v ) => a
          case None => a ++ computeClosure( x, a, next )
        }
      }
    } )
  }
  def next : Src => Set[Src]
  def key : Src => Key
  def close( src : Src ) : Map[Key,Set[Src]] = computeClosure( src, new HashMap[Key,Set[Src]](), next )
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
) extends RHOCTxn {
  override def equals( a : Any ) = {
    a match {
      case RHOCTxnRep( _, _, _, `hash`, _, _ ) => true
      case _ => false
    }
  }
  override def hashCode = ( hash ).##
}

trait Adjustment {
  def txn            : RHOCTxn
  def cleanBalance   : Float
  def taintedBalance : Float
}

class InitialAdjustment( val addr : String, override val taintedBalance : Float ) extends Adjustment {
  override def txn          = new InitialRHOCTxn( addr )
  override def cleanBalance = 0
}

case class ActualAdjustment( txn : RHOCTxn, cleanBalance : Float, taintedBalance : Float ) extends Adjustment

object RHOCTxnClosure extends Closure[String,RHOCTxn] {
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
  override def key = _.trgt

  def getBalance( addr : String ) : Float = {
    throw new Exception( "not implemented yet" )
  }
  def getTaint( addr : String ) : Float = {
    throw new Exception( "not implemented yet" )
  }

  def nextBalanceAdjustments( adjustment : Adjustment ) : Set[Adjustment] = {
    val txn = adjustment.txn
    val lowerCaseAddr = txn.trgt.toLowerCase()
    val etherscanURI =
      s"http://api.etherscan.io/api?module=account&action=tokentx&address=$lowerCaseAddr&startblock=0&endblock=999999999&sort=asc&apikey=$apiKey"
    val etherscanDataStr = httpClient.expect[String]( etherscanURI ).unsafeRunSync
    val etherscanJson = Ok( etherscanDataStr ).flatMap( _.as[Json] ).unsafeRunSync
    val etherscanTxnRslt = etherscanJson \\ "result"
    val etherscanTxnArray = etherscanTxnRslt( 0 ).asArray.getOrElse( throw new Exception( "not an array" ) )
    val rslt = etherscanTxnArray.foldLeft( new HashSet[Adjustment]() )(
      { ( acc, e ) => {
        val trgtAddr = ( e \\ "to" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
        val blockNumber = ( e \\ "blockNumber" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) ).toInt
        if ( ( trgtAddr != lowerCaseAddr )  && ( blockNumber <= blockHeight ) ){
          val srcAddr = ( e \\ "from" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
          val amt = ( e \\ "value" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) ).toFloat
          val hash = ( e \\ "hash" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
          val blockHash = ( e \\ "blockHash" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
          val clean = getBalance( txn.trgt )
          val taint = getTaint( txn.src )
          val newTxn = new RHOCTxnRep( srcAddr, trgtAddr, amt, hash, blockHash, new HashSet[RHOCTxn]() + txn )
            
          acc + new ActualAdjustment( newTxn, clean, taint )
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

  def closeAddr( addr : String ) = close( new InitialRHOCTxn( addr ) )
}




