package edf.utilities
import java.util.concurrent.Executors
object BatchConcurrency {

  //import scala.util._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent._
  import scala.concurrent.duration.Duration
  import scala.concurrent.duration.Duration._
  import edf.utilities.Holder
  import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
  import org.apache.spark.sql.functions.{col, lit}
  import edf.utilities.BatchConcurrency

  import scala.concurrent.{Await, Future}
  import scala.util.{Failure, Success, Try}

/*  val pool = Executors.newFixedThreadPool(5)
  @transient implicit val xc = ExecutionContext.fromExecutorService(pool)*/
  def executeAsync[T](f: => T): Future[T] = {
    Future(f)
  }

  def awaitSliding[T](it: Iterator[Future[T]], batchSize: Int = 10, timeout: Duration = Inf): Iterator[T] = {
    val slidingIterator = it.sliding(batchSize - 1).withPartial(true)
    //Holder.log.info("Inside the Continous Load 1")
    //Holder.log.info("Timestamp inside awaitSliding" + dummy)
    //Our look ahead (hasNext) will auto start the nth future in the batch
    val (initIterator, tailIterator) = slidingIterator.span(_ => slidingIterator.hasNext)
    initIterator.map(futureBatch => Await.result(futureBatch.head, timeout)) ++
      tailIterator.flatMap(lastBatch => Await.result(Future.sequence(lastBatch), timeout))
  }
}
