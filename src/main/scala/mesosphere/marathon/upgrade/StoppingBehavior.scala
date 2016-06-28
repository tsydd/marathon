package mesosphere.marathon.upgrade

import akka.actor.{ Actor, ActorLogging, Cancellable }
import akka.event.EventStream
import mesosphere.marathon.TaskUpgradeCanceledException
import mesosphere.marathon.core.task.{ Task, TaskStateOp }
import mesosphere.marathon.core.task.tracker.{ TaskStateOpProcessor, TaskTracker }
import mesosphere.marathon.event.MesosStatusUpdateEvent
import mesosphere.marathon.state.PathId
import mesosphere.marathon.upgrade.StoppingBehavior.KillNextBatch
import org.apache.mesos.SchedulerDriver

import scala.collection.mutable
import scala.concurrent.Promise
import scala.math.min

trait StoppingBehavior extends Actor with ActorLogging {
  import context.dispatcher

  def config: UpgradeConfig
  def driver: SchedulerDriver
  def eventBus: EventStream
  def promise: Promise[Unit]
  def taskTracker: TaskTracker
  def appId: PathId
  def stateOpProcessor: TaskStateOpProcessor
  var idsToKill: mutable.Set[Task.Id]
  var batchKill: mutable.Queue[Task.Id] = _
  var periodicalCheck: Cancellable = _

  private[this] def isLost(task: Task): Boolean = {
    import org.apache.mesos
    task.mesosStatus.fold(false)(_.getState == mesos.Protos.TaskState.TASK_LOST)
  }

  final override def preStart(): Unit = {
    val relevantTasks = taskTracker.appTasksLaunchedSync(appId).filter(t => idsToKill(t.taskId))
    val (lostTasks, launchedTasks) = relevantTasks.partition(isLost)

    // we only need to explicitly expunge lost tasks here. in case more tasks got lost, we'll receive a status update
    lostTasks.foreach { t =>
      // TODO: should we wait for the results? we couldn't really recover if an expunge fails ...
      stateOpProcessor.process(TaskStateOp.ForceExpunge(t.taskId))
    }

    // remove all lost tasks from the list because they have been expunged
    launchedTasks foreach { task =>
      idsToKill - task.taskId
    }

    eventBus.subscribe(self, classOf[MesosStatusUpdateEvent])
    periodicalCheck = context.system.scheduler.schedule(
      config.killBatchCycle, config.killBatchCycle, self, KillNextBatch)
    //initiate first batch
    batchKill = idsToKill.to[mutable.Queue]
    killNextTasks(config.killBatchSize)
    //check if there is anything to do
    checkFinished()
  }

  final override def postStop(): Unit = {
    periodicalCheck.cancel()
    eventBus.unsubscribe(self)
    if (!promise.isCompleted)
      promise.tryFailure(
        new TaskUpgradeCanceledException(
          "The operation has been cancelled"))
  }

  val taskFinished = "^TASK_(ERROR|FAILED|FINISHED|LOST|KILLED)$".r

  def receive: Receive = {
    case MesosStatusUpdateEvent(_, taskId, taskFinished(_), _, _, _, _, _, _, _, _) if idsToKill(taskId) =>
      idsToKill.remove(taskId)
      log.info(s"Task $taskId has been killed. Waiting for ${idsToKill.size} more tasks to be killed.")
      killNextTasks(1) //since one kill is processed, initiate another one
      checkFinished()

    case KillNextBatch =>
      if (batchKill.isEmpty && idsToKill.nonEmpty) synchronizeTasks()
      killNextTasks(config.killBatchSize)

    case x: MesosStatusUpdateEvent => log.debug(s"Received $x")
  }

  def killNextTasks(toKill: Int): Unit = {
    if (batchKill.nonEmpty) {
      val tasksToKill = (0 until min(toKill, batchKill.size)).map { _ => batchKill.dequeue().mesosTaskId }
      log.info(s"Killing ${tasksToKill.size} instances from ${idsToKill.size}")
      tasksToKill.foreach(driver.killTask)
    }
  }

  def synchronizeTasks(): Unit = {
    val trackerIds = taskTracker.appTasksLaunchedSync(appId).map(_.taskId).toSet
    idsToKill = idsToKill.filter(trackerIds)
    log.info(s"Synchronize tasks: ${idsToKill.size} instances to kill")
    batchKill = idsToKill.to[mutable.Queue]
    checkFinished()
  }

  def checkFinished(): Unit =
    if (idsToKill.isEmpty) {
      log.info("Successfully killed all the tasks")
      promise.success(())
      context.stop(self)
    }
}

object StoppingBehavior {
  case object KillNextBatch
}
