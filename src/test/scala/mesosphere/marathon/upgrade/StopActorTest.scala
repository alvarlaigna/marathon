package mesosphere.marathon
package upgrade

import akka.actor.{ ActorRef, Props }
import akka.testkit.TestActor.{ AutoPilot, NoAutoPilot }
import akka.testkit.TestProbe
import mesosphere.AkkaUnitTest
import mesosphere.marathon.upgrade.DeploymentActor.Cancel

import scala.concurrent.Promise

class StopActorTest extends AkkaUnitTest {

  "StopActor" should {
    "stop" in {
      val promise = Promise[Boolean]()
      val probe = TestProbe()

      probe.setAutoPilot(new AutoPilot {
        def run(sender: ActorRef, msg: Any): AutoPilot = msg match {
          case Cancel(reason) =>
            system.stop(probe.ref)
            NoAutoPilot
        }
      })
      val ref = system.actorOf(Props(classOf[StopActor], probe.ref, promise, new Exception("")))

      watch(ref)
      expectTerminated(ref)

      promise.future.futureValue should be(true)
    }
  }
}
