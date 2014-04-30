package sharding.example

import akka.remote.testkit.{MultiNodeSpec, MultiNodeConfig}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.apache.commons.io.FileUtils
import java.io.File
import akka.actor.{ActorIdentity, Identify, Props}
import akka.persistence.journal.leveldb.{SharedLeveldbJournal, SharedLeveldbStore}
import akka.remote.testconductor.RoleName
import akka.cluster.Cluster
import akka.contrib.pattern.ClusterSharding
import akka.testkit.ImplicitSender
import sharding.example.JobStreamRender.{SubscriberCount, Subscribe, Init}
import scala.concurrent.duration._
import scala.concurrent.Await


object JobStreamRenderConfig extends MultiNodeConfig {
   val node1 = role("node1") //will use it for db and test conductor
   val node2 = role("node2")
   val node3 = role("node3")
   val node4 = role("node4")

   commonConfig(ConfigFactory.parseString("""
     akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
     akka.persistence.journal.plugin = "akka.persistence.journal.leveldb-shared"
     akka.persistence.journal.leveldb-shared.store {
       native = off
       dir = "target/test-shared-journal"
     }
     akka.persistence.snapshot-store.local.dir = "target/test-snapshots"
     akka.cluster.auto-down-unreachable-after=5s
     akka.loglevel = INFO

   """))

  testTransport(true)

}

class JobStreamRenderSpecMultiJvmNode1 extends JobStreamRenderSpec
class JobStreamRenderSpecMultiJvmNode2 extends JobStreamRenderSpec
class JobStreamRenderSpecMultiJvmNode3 extends JobStreamRenderSpec
class JobStreamRenderSpecMultiJvmNode4 extends JobStreamRenderSpec


class JobStreamRenderSpec extends MultiNodeSpec(JobStreamRenderConfig)
  with WordSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender {

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()

  override def initialParticipants = roles.size

  private val storageLocations = List(
    "akka.persistence.journal.leveldb.dir",
    "akka.persistence.journal.leveldb-shared.store.dir",
    "akka.persistence.snapshot-store.local.dir").map(s => new File(system.settings.config.getString(s)))


  import JobStreamRenderConfig._

  //setting up the persistence store
  override protected def atStartup() {
    runOn(node1) {
      storageLocations.foreach(dir => FileUtils.deleteDirectory(dir))
    }
  }

  override protected def afterTermination() {
    runOn(node1) {
      storageLocations.foreach(dir => FileUtils.deleteDirectory(dir))
    }
  }


  "JobStream render app" must {

    "startup the shared journal for the jobstream" in within(15.seconds) {

      runOn(node1) {
        system.actorOf(Props[SharedLeveldbStore], "store")
      }
      enterBarrier("persistence-actor-started")

      //now setup the nodes that is going to use this journal actor for persisting messages
      runOn(node1, node2, node3, node4) {
        val ref1 = node(node1) / "user" / "store"
        system.actorSelection(ref1) ! Identify(None)
        val sharedStore = expectMsgType[ActorIdentity].ref.get
        SharedLeveldbJournal.setStore(sharedStore, system)
      }
      enterBarrier("persistence-started")
    }

    "startup the Shards" in within(15.seconds) {
       join(node1, node1)
       join(node2, node1)
       join(node3, node1)
       join(node4, node1)
       enterBarrier("sharding started")
    }

    "Run job stream render on node2 and subscribe from node4 " in within(15.seconds) {
      runOn(node2) {
        val region = ClusterSharding(system).shardRegion(JobStreamRender.shardName)
        region ! Init(100L)
      }
      enterBarrier("node2-jobstream-started")

      runOn(node4) {
        val region = ClusterSharding(system).shardRegion(JobStreamRender.shardName)
        region ! Subscribe(100L, "hey")
        expectMsg("Here you go...100")
      }
      enterBarrier("node4-subscribed-to-node2")
    }

    "Run job stream render on node3 and subscribe from node4 " in within(15.seconds) {
      runOn(node3) {
        val region = ClusterSharding(system).shardRegion(JobStreamRender.shardName)
        region ! Init(200L)
      }
      enterBarrier("node3-jobstream-started")

      runOn(node4) {
        val region = ClusterSharding(system).shardRegion(JobStreamRender.shardName)
        region ! Subscribe(200L, "hey")
        expectMsg("Here you go...200")
      }
      enterBarrier("node4-subscribed-to-node3")
    }

    "crash the job stream running on node2" in {
      runOn(node1) {
        //testconductor runs on node1
        testConductor.exit(node2, 0).await
      }
      enterBarrier("node2-is-down")
    }

    "access the crashed job stream from other node" in within(15.seconds) {
      runOn(node3) {
        val region = ClusterSharding(system).shardRegion(JobStreamRender.shardName)
        awaitAssert {
          within(2.seconds) {
            region ! Subscribe(200L, "hey")
            expectMsg("Here you go...200")
          }
        }
      }
      enterBarrier("jobstream-200-now-running-on-other-node")
    }

    "job stream recovers using persistence storage" in within(15.seconds) {
      runOn(node4) {
        val region = ClusterSharding(system).shardRegion(JobStreamRender.shardName)
        awaitAssert {
          within(2.second) {
            region ! SubscriberCount(100L)
            expectMsg(1)
          }
        }
      }
      enterBarrier("jobstream-100-is-recovered-using-persistence-storage")
    }
  }

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system) join node(to).address
      startSharding()
    }
    enterBarrier(from.name + "-joined")
  }

  private def startSharding(): Unit = {
    ClusterSharding(system).start(
      typeName = JobStreamRender.shardName,
      entryProps = Some(JobStreamRender.props()),
      idExtractor = JobStreamRender.idExtractor,
      shardResolver = JobStreamRender.shardResolver)
  }

}
