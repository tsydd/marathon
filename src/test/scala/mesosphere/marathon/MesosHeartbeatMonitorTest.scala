package mesosphere.marathon

import akka.testkit.TestProbe
import mesosphere.marathon.test.{ MarathonActorSupport, Mockito }
import mesosphere.marathon.util.heartbeat._
import org.apache.mesos._
import org.scalatest.{ BeforeAndAfterAll, Matchers }

class MesosHeartbeatMonitorTest extends MarathonActorSupport
    with MarathonSpec with BeforeAndAfterAll with Mockito with Matchers {

  class MonitorFactory {
    val heartbeatActor: TestProbe = TestProbe()
    val scheduler = mock[Scheduler]
    val reactor = mock[Heartbeat.Reactor]

    def newMonitor(): MesosHeartbeatMonitor = new MesosHeartbeatMonitor(scheduler, heartbeatActor.ref) {
      override def heartbeatReactor(driver: SchedulerDriver) = reactor
    }
  }

  test("MesosHeartbeatMonitor fully decorates Scheduler") {
    val factory = new MonitorFactory
    val monitor = factory.newMonitor

    monitor.registered(null, null, null)
    monitor.reregistered(null, null)
    monitor.resourceOffers(null, null)
    monitor.offerRescinded(null, null)
    monitor.statusUpdate(null, null)
    monitor.frameworkMessage(null, null, null, null)
    monitor.disconnected(null)
    monitor.slaveLost(null, null)
    monitor.executorLost(null, null, null, 0)
    monitor.error(null, null)

    verify(factory.scheduler, times(1)).registered(any, any, any)
    verify(factory.scheduler, times(1)).reregistered(any, any)
    verify(factory.scheduler, times(1)).resourceOffers(any, any)
    verify(factory.scheduler, times(1)).offerRescinded(any, any)
    verify(factory.scheduler, times(1)).statusUpdate(any, any)
    verify(factory.scheduler, times(1)).frameworkMessage(any, any, any, any)
    verify(factory.scheduler, times(1)).disconnected(any)
    verify(factory.scheduler, times(1)).slaveLost(any, any)
    verify(factory.scheduler, times(1)).executorLost(any, any, any, any)
    verify(factory.scheduler, times(1)).error(any, any)

    noMoreInteractions(factory.scheduler)
  }

  test("MesosHeartbeatMonitor sends proper actor messages for Scheduler callbacks") {
    val factory = new MonitorFactory
    val monitor = factory.newMonitor

    // activation messages
    val registeredDriver = mock[SchedulerDriver]
    monitor.registered(registeredDriver, null, null)
    factory.heartbeatActor.expectMsgType[Heartbeat.Message] should be(
      Heartbeat.MessageActivate(factory.reactor, MesosHeartbeatMonitor.sessionOf(registeredDriver)))

    val reregisteredDriver = mock[SchedulerDriver]
    monitor.reregistered(reregisteredDriver, null)
    factory.heartbeatActor.expectMsgType[Heartbeat.Message] should be(
      Heartbeat.MessageActivate(factory.reactor, MesosHeartbeatMonitor.sessionOf(reregisteredDriver)))

    // deactivation messages
    val disconnectedDriver = mock[SchedulerDriver]
    monitor.disconnected(disconnectedDriver)
    factory.heartbeatActor.expectMsgType[Heartbeat.Message] should be(
      Heartbeat.MessageDeactivate(MesosHeartbeatMonitor.sessionOf(disconnectedDriver)))

    val errorDriver = mock[SchedulerDriver]
    monitor.error(errorDriver, null)
    factory.heartbeatActor.expectMsgType[Heartbeat.Message] should be(
      Heartbeat.MessageDeactivate(MesosHeartbeatMonitor.sessionOf(errorDriver)))

    // pulse messages
    monitor.resourceOffers(null, null)
    factory.heartbeatActor.expectMsgType[Heartbeat.Message] should be(Heartbeat.MessagePulse)

    monitor.offerRescinded(null, null)
    factory.heartbeatActor.expectMsgType[Heartbeat.Message] should be(Heartbeat.MessagePulse)

    monitor.statusUpdate(null, null)
    factory.heartbeatActor.expectMsgType[Heartbeat.Message] should be(Heartbeat.MessagePulse)

    monitor.frameworkMessage(null, null, null, null)
    factory.heartbeatActor.expectMsgType[Heartbeat.Message] should be(Heartbeat.MessagePulse)

    monitor.slaveLost(null, null)
    factory.heartbeatActor.expectMsgType[Heartbeat.Message] should be(Heartbeat.MessagePulse)

    monitor.executorLost(null, null, null, 0)
    factory.heartbeatActor.expectMsgType[Heartbeat.Message] should be(Heartbeat.MessagePulse)
  }

  // TODO(jdef) test MesosHeartbeatMonitor.heartbeatReactor implementation
}
