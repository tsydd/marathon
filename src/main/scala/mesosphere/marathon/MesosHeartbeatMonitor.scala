package mesosphere.marathon

import java.util.UUID
import javax.inject.{ Inject, Named }

import akka.actor.ActorRef
import mesosphere.util.monitor._
import org.apache.mesos.{ Scheduler, SchedulerDriver }
import org.apache.mesos.Protos._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class MesosHeartbeatMonitor @Inject() (
    @Named(MesosHeartbeatMonitor.BASE) scheduler: Scheduler,
    @Named(ModuleNames.MESOS_HEARTBEAT_MONITOR) heartbeatMonitor: ActorRef
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
    heartbeatMonitor ! HeartbeatActor.MessageActivate(heartbeatReactor(driver))
    scheduler.registered(driver, frameworkId, master)
  }

  override def reregistered(driver: SchedulerDriver, master: MasterInfo): Unit = {
    heartbeatMonitor ! HeartbeatActor.MessageActivate(heartbeatReactor(driver))
    scheduler.reregistered(driver, master)
  }

  override def resourceOffers(driver: SchedulerDriver, offers: java.util.List[Offer]): Unit = {
    heartbeatMonitor ! HeartbeatActor.MessagePulse
    scheduler.resourceOffers(driver, offers)
  }

  override def offerRescinded(driver: SchedulerDriver, offer: OfferID): Unit = {
    heartbeatMonitor ! HeartbeatActor.MessagePulse
    scheduler.offerRescinded(driver, offer)
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
    heartbeatMonitor ! HeartbeatActor.MessagePulse
    scheduler.statusUpdate(driver, status)
  }

  override def frameworkMessage(
    driver: SchedulerDriver,
    executor: ExecutorID,
    slave: SlaveID,
    message: Array[Byte]): Unit = {
    heartbeatMonitor ! HeartbeatActor.MessagePulse
    scheduler.frameworkMessage(driver, executor, slave, message)
  }

  override def disconnected(driver: SchedulerDriver): Unit = {
    // heartbeat monitor may have (transiently) triggered this, but that's ok because if it did then
    // it's already "inactive", so this becomes a no-op
    heartbeatMonitor ! HeartbeatActor.MessageDeactivate
    scheduler.disconnected(driver)
  }

  override def slaveLost(driver: SchedulerDriver, slave: SlaveID): Unit = {
    heartbeatMonitor ! HeartbeatActor.MessagePulse
    scheduler.slaveLost(driver, slave)
  }

  override def executorLost(
    driver: SchedulerDriver,
    executor: ExecutorID,
    slave: SlaveID,
    code: Int): Unit = {
    heartbeatMonitor ! HeartbeatActor.MessagePulse
    scheduler.executorLost(driver, executor, slave, code)
  }

  override def error(driver: SchedulerDriver, message: String): Unit = {
    // errors from the driver are fatal (to the driver) so it should be safe to deactivate here because
    // the marathon scheduler **should** either exit or else create a new driver instance and reregister.
    heartbeatMonitor ! HeartbeatActor.MessageDeactivate
    scheduler.error(driver, message)
  }
}

object MesosHeartbeatMonitor {
  final val BASE = "mesosHeartbeatMonitor.base"
}
