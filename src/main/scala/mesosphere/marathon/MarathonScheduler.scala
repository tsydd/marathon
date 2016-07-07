package mesosphere.marathon

import javax.inject.{ Inject, Named }

import akka.actor.{ ActorRef, ActorSystem }
import akka.event.EventStream
import mesosphere.marathon.core.base.{ CurrentRuntime, Clock }
import mesosphere.marathon.core.launcher.OfferProcessor
import mesosphere.marathon.core.task.update.TaskStatusUpdateProcessor
import mesosphere.marathon.event._
import mesosphere.util.monitor._
import mesosphere.util.state.{ FrameworkIdUtil, MesosLeaderInfo }
import org.apache.mesos.Protos._
import org.apache.mesos.{ Scheduler, SchedulerDriver }
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.concurrent._
import scala.util.control.NonFatal
import scala.collection.JavaConverters._

trait SchedulerCallbacks {
  def disconnected(): Unit
}

class MarathonScheduler @Inject() (
    @Named(EventModule.busName) eventBus: EventStream,
    clock: Clock,
    offerProcessor: OfferProcessor,
    taskStatusProcessor: TaskStatusUpdateProcessor,
    frameworkIdUtil: FrameworkIdUtil,
    mesosLeaderInfo: MesosLeaderInfo,
    system: ActorSystem,
    config: MarathonConf,
    schedulerCallbacks: SchedulerCallbacks,
    @Named(ModuleNames.SCHEDULER_HEARTBEAT_MONITOR) heartbeatMonitor: ActorRef) extends Scheduler {

  private[this] val log = LoggerFactory.getLogger(getClass.getName)

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val zkTimeout = config.zkTimeoutDuration

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
    log.info(s"Registered as ${frameworkId.getValue} to master '${master.getId}'")
    heartbeatMonitor ! HeartbeatActor.MessageActivate(heartbeatReactor(driver))
    frameworkIdUtil.store(frameworkId)
    mesosLeaderInfo.onNewMasterInfo(master)
    eventBus.publish(SchedulerRegisteredEvent(frameworkId.getValue, master.getHostname))
  }

  override def reregistered(driver: SchedulerDriver, master: MasterInfo): Unit = {
    log.info("Re-registered to %s".format(master))
    heartbeatMonitor ! HeartbeatActor.MessageActivate(heartbeatReactor(driver))
    mesosLeaderInfo.onNewMasterInfo(master)
    eventBus.publish(SchedulerReregisteredEvent(master.getHostname))
  }

  override def resourceOffers(driver: SchedulerDriver, offers: java.util.List[Offer]): Unit = {
    import scala.collection.JavaConverters._
    heartbeatMonitor ! HeartbeatActor.MessagePulse
    offers.asScala.foreach { offer =>
      val processFuture = offerProcessor.processOffer(offer)
      processFuture.onComplete {
        case scala.util.Success(_) => log.debug(s"Finished processing offer '${offer.getId.getValue}'")
        case scala.util.Failure(NonFatal(e)) => log.error(s"while processing offer '${offer.getId.getValue}'", e)
      }
    }
  }

  override def offerRescinded(driver: SchedulerDriver, offer: OfferID): Unit = {
    log.info("Offer %s rescinded".format(offer))
    heartbeatMonitor ! HeartbeatActor.MessagePulse
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
    log.info("Received status update for task %s: %s (%s)"
      .format(status.getTaskId.getValue, status.getState, status.getMessage))

    heartbeatMonitor ! HeartbeatActor.MessagePulse
    taskStatusProcessor.publish(status).onFailure {
      case NonFatal(e) =>
        log.error(s"while processing task status update $status", e)
    }
  }

  override def frameworkMessage(
    driver: SchedulerDriver,
    executor: ExecutorID,
    slave: SlaveID,
    message: Array[Byte]): Unit = {
    log.info("Received framework message %s %s %s ".format(executor, slave, message))
    heartbeatMonitor ! HeartbeatActor.MessagePulse
    eventBus.publish(MesosFrameworkMessageEvent(executor.getValue, slave.getValue, message))
  }

  override def disconnected(driver: SchedulerDriver) {
    log.warn("Disconnected")

    // heartbeat monitor may have (transiently) triggered this, but that's ok because if it did then
    // it's already "inactive", so this becomes a no-op
    heartbeatMonitor ! HeartbeatActor.MessageDeactivate

    eventBus.publish(SchedulerDisconnectedEvent())

    // Disconnection from the Mesos master has occurred.
    // Thus, call the scheduler callbacks.
    schedulerCallbacks.disconnected()
  }

  override def slaveLost(driver: SchedulerDriver, slave: SlaveID) {
    log.info(s"Lost slave $slave")
    heartbeatMonitor ! HeartbeatActor.MessagePulse
  }

  override def executorLost(
    driver: SchedulerDriver,
    executor: ExecutorID,
    slave: SlaveID,
    p4: Int) {
    log.info(s"Lost executor $executor slave $p4")
    heartbeatMonitor ! HeartbeatActor.MessagePulse
  }

  override def error(driver: SchedulerDriver, message: String) {
    log.warn(s"Error: $message\n" +
      s"In case Mesos does not allow registration with the current frameworkId, " +
      s"delete the ZooKeeper Node: ${config.zkPath}/state/framework:id\n" +
      s"CAUTION: if you remove this node, all tasks started with the current frameworkId will be orphaned!")

    heartbeatMonitor ! HeartbeatActor.MessageDeactivate

    // Currently, it's pretty hard to disambiguate this error from other causes of framework errors.
    // Watch MESOS-2522 which will add a reason field for framework errors to help with this.
    // For now the frameworkId is removed based on the error message.
    val removeFrameworkId = message match {
      case "Framework has been removed" => true
      case _: String => false
    }
    suicide(removeFrameworkId)
  }

  /**
    * Exits the JVM process, optionally deleting Marathon's FrameworkID
    * from the backing persistence store.
    *
    * If `removeFrameworkId` is set, the next Marathon process elected
    * leader will fail to find a stored FrameworkID and invoke `register`
    * instead of `reregister`.  This is important because on certain kinds
    * of framework errors (such as exceeding the framework failover timeout),
    * the scheduler may never re-register with the saved FrameworkID until
    * the leading Mesos master process is killed.
    */
  protected def suicide(removeFrameworkId: Boolean): Unit = {
    log.error(s"Committing suicide!")

    if (removeFrameworkId) Await.ready(frameworkIdUtil.expunge(), config.zkTimeoutDuration)

    // Asynchronously call asyncExit to avoid deadlock due to the JVM shutdown hooks
    CurrentRuntime.asyncExit()
  }
}
