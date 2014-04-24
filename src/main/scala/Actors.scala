package sharding.example

import akka.actor._
import akka.contrib.pattern.{ClusterSharding, ShardRegion}

import akka.routing.RoundRobinPool
import akka.cluster.routing.{ClusterRouterPoolSettings, ClusterRouterPool}
import akka.persistence.EventsourcedProcessor
import scala.collection.mutable.ListBuffer

object JobStreamRender {

  case class Subscribe(jobStreamId: Long, payload: Any)

  private case class Subscriber(ref: ActorRef, payload: Any)

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

  var subsribers = ListBuffer.empty[ActorRef]

  val renderers = context.actorOf(
    ClusterRouterPool(RoundRobinPool(0), ClusterRouterPoolSettings(
      totalInstances = 100, maxInstancesPerNode = 3,
      allowLocalRoutees = false, useRole = None)).props(Props[ActiveSector]),
    name = "renderRouter")

  private def updateState(c: Subscriber): Unit = subsribers += c.ref

  override def receiveRecover: Receive = {
    case e: Subscriber => updateState(e)
  }

  def receiveCommand = {
    case s@Subscribe(jobStreamId, mapping) =>
      println(s">>>>>> Processing ${s}")
      persist(Subscriber(sender, mapping))(updateState)
      renderers.forward('render)

    case 'Fail => throw new RuntimeException("!!!! CRASHED")
  }
}

class ActiveSector extends Actor {

  def receive = {
    case 'polygon =>
       sender ! "Here you go..."
  }
}


class JobStreamRouter extends Actor {
  import scala.concurrent.duration._
  import JobStreamRender._

  context.setReceiveTimeout(100.milliseconds)

  val region = ClusterSharding(context.system).shardRegion(JobStreamRender.shardName)

  def receive: Receive = {
    case s: Subscribe =>
      region.forward(s)

    case ReceiveTimeout =>
      region ! Subscribe(100, "show me a new car")
      region ! Subscribe(100, "show me money")
      region ! Subscribe(200, "surprise me!")
  }
}





