package sharding.example

import akka.actor._
import akka.contrib.pattern.{ClusterSharding, ShardRegion}

import scala.collection.mutable.ListBuffer
import akka.routing.RoundRobinPool
import akka.cluster.routing.{ClusterRouterPoolSettings, ClusterRouterPool}
import akka.persistence.EventsourcedProcessor

object JobStreamRender {

  case class Subscribe(jobStreamId: Long, mapping: Any)

  case class CounterChanged(delta: Int)

  val idExtractor: ShardRegion.IdExtractor = {
    case s: Subscribe  => (s.jobStreamId.toString, s)
  }

  val shardResolver: ShardRegion.ShardResolver = msg => msg match {
    case s: Subscribe  => (s.jobStreamId % 100).toString
  }

  val shardName: String = "JobStreamRender"

  def props(): Props = Props[JobStreamRender]
}


class JobStreamRender extends EventsourcedProcessor {

  import JobStreamRender._
  var counter = 0

  val workerRouter = context.actorOf(
    ClusterRouterPool(RoundRobinPool(0), ClusterRouterPoolSettings(
      totalInstances = 100, maxInstancesPerNode = 3,
      allowLocalRoutees = false, useRole = None)).props(Props[ActiveSector]),
    name = "workerRouter3")

  private def updateState(c: CounterChanged) = counter += c.delta

  override def receiveRecover: Receive = {
    case e: CounterChanged =>
      println(">>>>>>>>> Recovering from the failure")
      updateState(e)
  }

  def receiveCommand = {
    case Subscribe(jobStreamId, mapping) =>
      println(s">>>>>>>>>>>> JobStreamRender: Received message from ${sender} to ${self}, state = ${counter}")
      persist(CounterChanged(+1))(updateState)
      if(counter == 10) { throw new RuntimeException("CRASHED!!!!!!!!!!!!!!")}
      workerRouter ! 'polygon

  }

}

class ActiveSector extends Actor {

  def receive = {
    case 'polygon =>
      //println(s">>>>>>>>> ActiveSector: Called from ${sender} to ${self}")
  }
}


class Bot extends Actor {
  import scala.concurrent.duration._
  import JobStreamRender._

  context.setReceiveTimeout(500.milliseconds)

  val region = ClusterSharding(context.system).shardRegion(JobStreamRender.shardName)

  def receive: Receive = {
    case ReceiveTimeout =>
      region ! Subscribe(100, "foo")
      region ! Subscribe(100, "foo")
      region ! Subscribe(200, "bar")
  }
}





