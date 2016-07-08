package mesosphere.marathon

import java.util.UUID
import javax.inject.{ Inject, Named }

import akka.actor.ActorRef
import mesosphere.marathon.util.heartbeat._
import org.apache.mesos.{ Scheduler, SchedulerDriver }
import org.apache.mesos.Protos._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

// @constructor create a mesos Scheduler decorator that intercepts callbacks from a mesos SchedulerDriver,
// translating each callback into some kind of HeartbeatMonitor.Message that is asynchronously delivered to a
// heartbeatActor. All callback parameters are passed through, unchanged, to the given delegate scheduler
// implementation.
//
// @param scheduler is the delegate scheduler implementation
// @param heartbeatActor is the receipient of generated HeartbeatActor.Message's
//
// @see mesosphere.util.monitor.HeartbeatMonitor
// @see org.apache.mesos.Scheduler
// @see org.apache.mesos.SchedulerDriver
class MesosHeartbeatMonitor @Inject() (
    @Named(MesosHeartbeatMonitor.BASE) scheduler: Scheduler,
    @Named(ModuleNames.MESOS_HEARTBEAT_ACTOR) heartbeatActor: ActorRef
) extends Scheduler {

  private[this] val log = LoggerFactory.getLogger(getClass.getName)

  log.info(s"using mesos heartbeat monitor for scheduler $scheduler")

  protected def heartbeatReactor(driver: SchedulerDriver): HeartbeatActor.Reactor = new HeartbeatActor.Reactor {
    // virtualHeartbeatTasks is sent in a reconciliation message to mesos in order to force a
    // predictable response: the master (if we're connected) will send back a TASK_LOST because
    // the fake task ID and agent ID that we use will never actually exist in the cluster.
    // this is part of a short-term workaround: will no longer be needed once marathon is ported
    // to use the new mesos v1 http API.
    private[this] val virtualHeartbeatTasks: java.util.Collection[TaskStatus] = Seq(
      TaskStatus.newBuilder
      .setTaskId(TaskID.newBuilder.setValue("fake-marathon-pacemaker-task-" + UUID.randomUUID().toString))
      .setState(TaskState.TASK_LOST) // required, so we just need to set something
      .setSlaveId(SlaveID.newBuilder.setValue("fake-marathon-pacemaker-agent-" + UUID.randomUUID().toString))
      .build
    ).asJava

    override def onSkip(): Unit = {
      driver.reconcileTasks(virtualHeartbeatTasks)
    }

    override def onFailure(): Unit = {
      log.warn("Too many subsequent heartbeats missed; inferring disconnected from mesos master")
      disconnected(driver)
    }
  }

  override def registered(
    driver: SchedulerDriver,
    frameworkId: FrameworkID,
    master: MasterInfo): Unit = {
    heartbeatActor ! HeartbeatActor.MessageActivate(heartbeatReactor(driver), driver)
    scheduler.registered(driver, frameworkId, master)
  }

  override def reregistered(driver: SchedulerDriver, master: MasterInfo): Unit = {
    heartbeatActor ! HeartbeatActor.MessageActivate(heartbeatReactor(driver), driver)
    scheduler.reregistered(driver, master)
  }

  override def resourceOffers(driver: SchedulerDriver, offers: java.util.List[Offer]): Unit = {
    heartbeatActor ! HeartbeatActor.MessagePulse
    scheduler.resourceOffers(driver, offers)
  }

  override def offerRescinded(driver: SchedulerDriver, offer: OfferID): Unit = {
    heartbeatActor ! HeartbeatActor.MessagePulse
    scheduler.offerRescinded(driver, offer)
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
    heartbeatActor ! HeartbeatActor.MessagePulse
    scheduler.statusUpdate(driver, status)
  }

  override def frameworkMessage(
    driver: SchedulerDriver,
    executor: ExecutorID,
    slave: SlaveID,
    message: Array[Byte]): Unit = {
    heartbeatActor ! HeartbeatActor.MessagePulse
    scheduler.frameworkMessage(driver, executor, slave, message)
  }

  override def disconnected(driver: SchedulerDriver): Unit = {
    // heartbeatReactor may have triggered this, but that's ok because if it did then
    // it's already "inactive", so this becomes a no-op
    heartbeatActor ! HeartbeatActor.MessageDeactivate(driver)
    scheduler.disconnected(driver)
  }

  override def slaveLost(driver: SchedulerDriver, slave: SlaveID): Unit = {
    heartbeatActor ! HeartbeatActor.MessagePulse
    scheduler.slaveLost(driver, slave)
  }

  override def executorLost(
    driver: SchedulerDriver,
    executor: ExecutorID,
    slave: SlaveID,
    code: Int): Unit = {
    heartbeatActor ! HeartbeatActor.MessagePulse
    scheduler.executorLost(driver, executor, slave, code)
  }

  override def error(driver: SchedulerDriver, message: String): Unit = {
    // errors from the driver are fatal (to the driver) so it should be safe to deactivate here because
    // the marathon scheduler **should** either exit or else create a new driver instance and reregister.
    heartbeatActor ! HeartbeatActor.MessageDeactivate(driver)
    scheduler.error(driver, message)
  }
}

object MesosHeartbeatMonitor {
  final val BASE = "mesosHeartbeatMonitor.base"

  final val DEFAULT_HEARTBEAT_INTERVAL_MS = 15000L
  final val DEFAULT_HEARTBEAT_FAILURE_THRESHOLD = 5
}
