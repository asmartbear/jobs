import { StatusManager } from '@asmartbear/status';
import { cpus as getNumCpus } from 'os';
import { Semaphore, E_CANCELED, Mutex } from 'async-mutex';

/**
 * Task execution state.  Goes from `New` to `Running` to `Done` or `Error`.
 */
export enum TaskState {
  New,
  Running,
  Done,
  Error
}

/**
 * Exception thrown by a task, including information about the task.
 * 
 * You can use `this.thrown` to find the original object that was throws, and `this.task` for the task that threw it.
 * The standard `Error` message will include the task title and the message from the original error.
 */
export class TaskError<Tags extends string> extends Error {
  constructor(public readonly task: Task<Tags>, public readonly thrown: unknown) {
    super(`Exception while processing task ${task.title}: ${thrown instanceof Error ? thrown.message : String(thrown)}`);
  }
}

/**
 * Use to send status update messages about the task; will be ignored if not in verbose mode.
 * 
 * @param msg The message to display
 * @param immediate (default `false`) Should this message be displayed immediately and blocking? Otherwise, waits for a backoff.
 */
export type TaskUpdateFunction = (msg: string, immediate?: boolean) => void;

/**
 * Task execution function.  This is the function that actually does the work of the task.
 * We don't care about the return value, but we do care if it's wrapped in a Promise, but still don't care what the promise resolves to.
 */
export type TaskExecutionFunction = (fStatus: TaskUpdateFunction) => Promise<unknown> | unknown;

/**
 * Promise-based time-wait function.
 */
function wait(ms: number): Promise<void> {
  return new Promise(success => setTimeout(success, ms))
}

export type TaskRunnerConstructor<Tags extends string> = {

  /**
   * Use the console to show status of running jobs?
   */
  showStatus?: boolean,

  /**
   * In status updates, show the worker index from the list?
   */
  showWorkerIdx?: boolean,

  /**
   * The total concurrency level, or undefined to use the number of CPUs available.
   */
  concurrencyLevel?: number,

  /**
   * The maximum concurrency level per tag; missing tags are unlimited.
   */
  concurrencyPerTag?: Partial<Record<Tags, number>>,
}

/**
 * Options when creating a new `Task`.
 */
export type TaskConstructor<Tags extends string> = {

  /**
   * Used for human-readable output.
   */
  title: string,

  /**
   * Tags to apply to this task.
   */
  tags?: readonly Tags[],

  /**
   * If there are tasks tagged with any of these, we wait for them to complete before we can run.
   */
  dependentTags?: readonly Tags[],

  /**
   * Tasks that must complete before this one can run.
   */
  dependentTasks?: readonly Task<Tags>[],

  /**
   * Priority order, lower numbers run first.  Ties are broken arbitrarily.  All numbers are allowed.
   * If not provided, the effective value is `0`.
   */
  priority?: number,
}

/**
 * Runnable Task.
 */
export class Task<Tags extends string> {

  /**
   * Used for human-readable output.
   */
  public readonly title: string

  /**
   * Tags that are applied to this task.
   */
  public readonly tags: readonly Tags[]

  /**
   * If there are tasks tagged with any of these, we wait for them to complete before we can run.
   */
  private readonly dependentTags: readonly Tags[]

  /**
   * Tasks that must complete before this one can run.
   */
  private readonly dependentTasks: Task<Tags>[]

  /**
   * Priority order, lower numbers run first.  Ties are broken arbitrarily.  Default is `0`.
   */
  public readonly priority: number

  /**
   * The current state of the Task.
   */
  public state: TaskState = TaskState.New;

  /**
   * Used to signal waiting threads that this task has completed.
   */
  private readonly completionMutex = new Mutex()

  constructor(config: TaskConstructor<Tags>, private readonly executionFunction: TaskExecutionFunction) {
    this.title = config.title
    this.tags = config.tags ?? []
    this.dependentTags = config.dependentTags ?? []
    this.dependentTasks = config.dependentTasks ? Array.from(config.dependentTasks) : []
    this.priority = config.priority ?? 0
    if (this.tags.length > 0) {
      this.title += " [" + this.tags.join(", ") + "]"
    }
    this.completionMutex.acquire()    // by taking the mutex, every other thread will block if they attempt to acquire it.
  }

  /**
   * True if this task is ready to run now, based on any configuration or rules, which can even be dynamic.
   */
  isReady(runner: TaskRunner<Tags>): boolean {

    // Have to be "new" to be ready
    if (this.state !== TaskState.New) return false;

    // If any task we're dependent on isn't done, we're not ready
    if (this.dependentTasks.some(task => task.state !== TaskState.Done)) return false;

    // Check tag-based completion dependency
    if (this.dependentTags.some(tag =>
      (runner.numQueuedTasksByTag.get(tag) ?? 0) + (runner.numRunningTasksByTag.get(tag) ?? 0) > 0
    )) return false;

    // Check concurrency-based tags
    for (const tag of this.tags) {
      const concurrency = runner.config.concurrencyPerTag?.[tag] ?? 9999;
      const current = runner.numRunningTasksByTag.get(tag) ?? 0
      if (current >= concurrency) return false;
    }

    return true;
  }

  /**
   * Executes this Task, returning any `Error` thrown, or `null` if nothing was thrown.
   */
  async execute(fStatus: TaskUpdateFunction): Promise<TaskError<Tags> | null> {
    this.state = TaskState.Running;
    try {
      await this.executionFunction(fStatus);
      this.state = TaskState.Done;
      return null;
    } catch (error) {
      this.state = TaskState.Error;
      return new TaskError(this, error);
    } finally {
      this.completionMutex.release()    // release anyone waiting for this task to complete
    }
  }

  /**
   * Waits for this task to complete, or returns immediately if the task was already complete.
   * 
   * @returns `this` for chaining, but as a Promise because generally we're waiting for the completion.
   */
  async waitForCompletion(): Promise<Task<Tags>> {
    if (this.state == TaskState.Done || this.state == TaskState.Error) return this    // already complete
    await this.completionMutex.acquire()    // wait
    this.completionMutex.release()    // let the next thread see the signal
    return this
  }

  /**
   * Declares that we're dependent on the completion of some other task.
   */
  addDependentTask(task: Task<Tags>): void {
    this.dependentTasks.push(task);
  }
}

/**
 * A Task-running system.  Load initial tasks; tasks can beget other tasks.  Can run until all are complete.
 */
export class TaskRunner<Tags extends string> {
  public readonly concurrencyLevel: number;
  private status: StatusManager<number> | null;

  /**
   * Tasks which were ready to run the last time we checked.  This status can change though!
   */
  private readyQueue: Task<Tags>[] = [];

  /**
   * Tasks which were not ready to run the last time we checked.  This can change at any time!
   */
  private waitingQueue: Task<Tags>[] = [];

  /**
   * Unordered list of tasks that are currently running.
   */
  private runningTasks: Task<Tags>[] = [];

  /**
   * Number of tasks queued to run, by tag.  Includes both ready and waiting queues.
   */
  public numQueuedTasksByTag = new Map<Tags, number>()

  /**
   * Number of tasks currently running, by tag.
   */
  public numRunningTasksByTag = new Map<Tags, number>()

  /**
   * Total number of tasks that were running, and completed.  Whether successfully or with error.
   */
  private numTasksCompleted = 0

  /**
   * A flag that says we should keep running even if there are no waiting or running tasks.
   */
  private stayAlive: boolean = false

  /**
   * Sempahore for the ready queue.
   */
  private readySemaphore = new Semaphore(0)

  /**
   * If a task had an error, this is non-null with the error.
   */
  private _error: TaskError<Tags> | null = null;

  constructor(public readonly config: TaskRunnerConstructor<Tags>) {
    this.concurrencyLevel = config.concurrencyLevel ?? getNumCpus().length;
    this.status = config.showStatus ? new StatusManager() : null
  }

  /**
   * Gets the number of tasks registered but not running, either because we haven't started yet,
   * or they are blocked by dependencies, or they are ready but there's no worker available,
   * or they are currently running.
   */
  get numUnfinishedTasks(): number {
    return this.waitingQueue.length + this.runningTasks.length + this.readyQueue.length
  }

  /**
   * Gets the number of tasks currently running in workers.
   * Will never be more than the number of workers.
   */
  get numRunningTasks(): number {
    return this.runningTasks.length
  }

  /**
   * Turns status on or off after creation.  Does nothing if it's already in that state.
   */
  statusControl(isEnabled: boolean) {
    if (isEnabled && !this.status) {
      this.status = new StatusManager();
      this.status.start()
    } else if (!isEnabled && this.status) {
      this.status.stop();
      this.status = null;
    }
  }

  /**
   * Creates a tag, enqueues it to run, and returns it.
   */
  addTask(config: TaskConstructor<Tags>, executionFunction: TaskExecutionFunction): Task<Tags> {
    const task = new Task(config, executionFunction);
    TaskRunner.updateTagCounter(task, this.numQueuedTasksByTag, 1);
    // Add to the appropriate queue
    if (task.isReady(this)) {
      this.addToReady(task)
    } else {
      this.waitingQueue.push(task)
    }
    return task
  }

  /**
   * If in verbose mode, updates the status that goes with a specific task
   */
  private updateStatus(statusIdx: number, msg: string, immediate: boolean) {
    if (this.status) {
      this.status.update(statusIdx, msg, immediate)
    }
  }

  /**
   * Updates the given counter for all the tags in the given task, incrementing by the given amount.
   */
  private static updateTagCounter<Tags extends string>(task: Task<Tags>, counter: Map<Tags, number>, increment: number) {
    for (const tag of task.tags) {
      counter.set(tag, (counter.get(tag) ?? 0) + increment)
    }
  }

  /**
   * Adds the given task to the ready queue, which also releases the semaphore.
   */
  private addToReady(task: Task<Tags>) {
    this.readyQueue.push(task)
    this.readyQueue.sort((a, b) => a.priority - b.priority)   // FIXME: Presumably faster to insert after binary search
    this.readySemaphore.release()
  }

  /**
   * If `true`, the workers will remain running even if there are no jobs left either running or waiting.
   * This is useful when there can be gaps in adding tasks.  Set to `false` eventually, or the job-runner
   * will never exit.
   */
  public setStayAlive(x: boolean) {
    this.stayAlive = x
    if (!x) {
      // If all workers are waiting because of this flag, and we just cleared it, they need to wake up
      // so that they can exit. They don't need this if there's more tasks to run, only if this flag
      // was the _only_ thing keeping them going.
      if (!this.numUnfinishedTasks) {
        this.readySemaphore.cancel()
      }
    }
  }

  /**
   * Runs all tasks until completion.  Tasks can add more tasks.
   * Once finished, check `this.error` for whether there were problems.
   */
  async run(): Promise<void> {

    // Announce the start
    if (this.status) {
      console.log(`Jobs starting; pid=${process.pid}; concurrency=${this.concurrencyLevel}`);
      this.status.start()
    }

    // Execute and time all workers
    const tStart = Date.now();
    const workers = Array(this.concurrencyLevel).fill(null).map((_, i) => this.worker(i));
    await Promise.all(workers);
    const tEnd = Date.now();

    // Announce the end
    if (this.status) {
      this.status.stop()
      const durationMs = tEnd - tStart
      const durationStr = durationMs < 4000 ? `${durationMs}ms` : `${Math.ceil(durationMs / 1000)}s`
      console.log(`Jobs finished; pid=${process.pid}; ${this.numTasksCompleted} tasks completed in ${durationStr}`);
    }
  }

  /**
   * Waits until all tasks containing any of a set of tags have completed.
   * New tasks with these tags might appear while waiting; it will still wait for all of them.
   * If there's a gap when no tasks have these tags, it will complete; if more tasks are added
   * afterwards with those tags, that's too bad -- it couldn't have known that!
   * 
   * @return true if we're still running normally and all those tasks have completed; false if we stopped early due to error.
   */
  async waitForTags(tags: Tags[]): Promise<boolean> {
    while (!this._error) {    // terminate on error
      // If any tasks are queued or running, that contain any of these tags, we're not finished waiting.
      if (!tags.some(tag =>
        (this.numQueuedTasksByTag.get(tag) ?? 0) + (this.numRunningTasksByTag.get(tag) ?? 0) > 0
      )) {
        return !this._error
      }
      // Wait a short while.  (This could be better if we had some event we could wait for!)
      await wait(10)
    }
    return false
  }

  /**
   * Runs one worker until queues are finished.
   */
  private async worker(statusIdx: number): Promise<void> {
    let hasDoneAnything = false   // don't emit messages until we've actually done something, so we don't take a slot on the command-line
    // Our own status function that only updates status if we've done something, and uses our worker index as a key
    const fStatus: TaskUpdateFunction = (msg, immediate) => (hasDoneAnything && this.updateStatus(statusIdx, msg, immediate ?? false))
    while (this._error === null && (this.stayAlive || this.numUnfinishedTasks > 0)) {

      try {
        // Wait to acquire the semaphore, which means something is ready to run.
        if (this.readySemaphore.isLocked()) {
          // If we believe we'll be waiting, it's worth updating the status.
          // Otherwise, we're about to run something, so don't bother flickering the screen.
          fStatus("💤", true)
        }
        await this.readySemaphore.acquire(1, -statusIdx)   // prioritize existing workers so we don't create more slots than actually needed
      } catch (err) {
        if (err === E_CANCELED) {
          // Expected!  This is the end of the job queue, and we've been awoken so we can exit normally
          break
        }
        throw err     // unexpected!
      }

      // Grab the next task that's ready to run.
      // There should always be one because of the semaphore, but just in case, check, and loop around and wait if not.
      const task = this.readyQueue.shift()
      if (!task) {
        // console.log("WAIT???")
        continue    // will either exit the loop or wait for the semaphore again
      }

      // It's also possible that the task became unready while it was queued to run.
      // Check for this case, and if so, put it back in the waiting queue and loop around to get another.
      if (!task.isReady(this)) {
        // console.log("became unready")
        this.waitingQueue.unshift(task)
        continue
      }

      // Prepare stats and lists for running the task
      hasDoneAnything = true
      fStatus(`🏃‍♂️ ${task.title}`, true)
      this.runningTasks.push(task);
      TaskRunner.updateTagCounter(task, this.numQueuedTasksByTag, -1)
      TaskRunner.updateTagCounter(task, this.numRunningTasksByTag, 1)

      // Execute the task
      const tStart = Date.now()
      const error = await task.execute((msg, immediate) => this.updateStatus(statusIdx, `🏃‍♂️ ${task.title}: ${msg}`, immediate ?? false));
      const tDuration = Date.now() - tStart

      // Update stats and lists
      this.runningTasks = this.runningTasks.filter(t => t !== task);
      TaskRunner.updateTagCounter(task, this.numRunningTasksByTag, -1)
      ++this.numTasksCompleted

      // Emit completion message
      if (this.status) {
        let msg = `Completed: ${task.title} in ${tDuration}ms`
        if (this.config.showWorkerIdx) {
          msg += ` by [${statusIdx}]`
        }
        if (error) {
          msg += ` (ERR: ${error.message})`
        }
        console.log(msg)
      }
      if (error) {
        this._error = error;
      }

      // Whenever a task completes, it's possible that some waiting tasks are now ready.
      // Find as many as we can and move them to the ready queue.
      for (var i = this.waitingQueue.length; --i >= 0;) {   // count backwards so we can remove items as we go
        if (this.waitingQueue[i].isReady(this)) {
          this.addToReady(this.waitingQueue.splice(i, 1)[0])
        }
      }
    }

    // Bye bye message!
    fStatus("✌️", true)

    // Got here because we're totally done.
    // Some workers might still be going; that's fine.
    // Wake the remaining ones so they can exit normally
    this.readySemaphore.cancel()
  }

  /**
   * The first error encountered, if any.
   */
  get error(): TaskError<Tags> | null {
    return this._error;
  }

}

// Usage example
// async function main() {
//   const manager = new TaskRunner<"foo" | "bar">({
//     concurrencyLevel: 6,
//     showStatus: true,
//     showWorkerIdx: true,
//     concurrencyPerTag: {
//       'foo': 3,
//     }
//   });

//   const mainTask = manager.addTask({
//     title: "THE MAIN ONE",
//   }, async () => {
//     await new Promise(resolve => setTimeout(resolve, 1000));
//   })

//   const the20s: Task<"foo" | "bar">[] = []
//   for (let i = 0; i < 35; ++i) {
//     const tagged = i % 2 == 1
//     const task = manager.addTask({
//       title: `Task ${i}`,
//       tags: tagged ? ["foo"] : ["bar"],
//       dependentTags: tagged ? [] : ["foo"],
//       dependentTasks: [mainTask],
//       priority: i,
//     }, async (fStatus) => {
//       fStatus("Wait A")
//       await new Promise(resolve => setTimeout(resolve, Math.random() * 500));
//       fStatus("Wait B")
//       await new Promise(resolve => setTimeout(resolve, Math.random() * 500));
//       fStatus("Wait C")
//       await new Promise(resolve => setTimeout(resolve, Math.random() * 500));
//       fStatus("Done")
//     });
//     if (i >= 20 && i < 30) {
//       the20s.push(task)
//     }
//   }
//   manager.setStayAlive(true)
//   setTimeout(() => manager.setStayAlive(false), 15000)

//   await Promise.all([
//     manager.run(),
//     Promise.all(the20s.map(task => task.waitForCompletion())).then(() => console.log("the 20s are done")),
//     manager.waitForTags(['foo']).then(() => { console.log("foo tasks are done") })
//   ]);

//   if (manager.error) {
//     console.error("An error occurred:", manager.error);
//   } else {
//     console.log("All tasks completed successfully");
//   }
// }

// main().catch(console.error);