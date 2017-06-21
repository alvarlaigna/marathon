package mesosphere.marathon
package core.election

import java.net.UnknownHostException
import mesosphere.AkkaUnitTest
import mesosphere.marathon.util.ScallopStub
import org.scalatest.Inside

import scala.concurrent.duration._
import scala.util.{ Failure, Success }

class CuratorElectionStreamTest extends AkkaUnitTest with Inside {
  "CuratorElectionStream" should {
    "fail the stream when given an unresolvable hostname" in {
      val conf = new ZookeeperConf {
        override lazy val zooKeeperUrl = ScallopStub(Some("zk://unresolvable:8080/marathon"))
        override lazy val zooKeeperSessionTimeout = ScallopStub(Some(1000L))
        override lazy val zooKeeperConnectionTimeout = ScallopStub(Some(1000L))
        override lazy val zkSessionTimeoutDuration = 10000.milliseconds
        override lazy val zkConnectionTimeoutDuration = 10000.milliseconds
        override lazy val zkTimeoutDuration = 250.milliseconds
      }

      val service = CuratorElectionStream(conf, "80")(system)
      inside(service.runForeach(println)
        .map(Success(_))
        .recover { case ex => Failure(ex) }
        .futureValue) {
        case Failure(ex: UnknownHostException) =>
          ex shouldBe a[UnknownHostException]
      }
    }
  }
}
