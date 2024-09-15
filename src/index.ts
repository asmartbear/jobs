import { StatusManager } from '@asmartbear/status';
import { cpus as getNumCpus } from 'os';
import { Semaphore, E_CANCELED } from 'async-mutex';

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
 * Use to send status update messages about the task; will be ignored if not in verbose mode.
 */
export type TaskUpdateFunction = (msg: string) => void;

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
  tags?: Tags[]

  /**
   * If there are tasks tagged with any of these, we wait for them to complete before we can run.
   */
  dependentTags?: Tags[]

  /**
   * Tasks that must complete before this one can run.
   */
  dependentTasks?: Task<Tags>[]
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
  public readonly tags: Tags[]

  /**
   * If there are tasks tagged with any of these, we wait for them to complete before we can run.
   */
  private readonly dependentTags: Tags[]

  /**
   * Tasks that must complete before this one can run.
   */
  private readonly dependentTasks: Task<Tags>[] = [];

  /**
   * The current state of the Task.
   */
  public state: TaskState = TaskState.New;

  constructor(config: TaskConstructor<Tags>, private readonly executionFunction: (fStatus: TaskUpdateFunction) => Promise<void>) {
    this.title = config.title
    this.tags = config.tags ?? []
    this.dependentTags = config.dependentTags ?? []
    this.dependentTasks = config.dependentTasks ?? []
    if (this.tags.length > 0) {
      this.title += " [" + this.tags.join(", ") + "]"
    }
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
  async execute(fStatus: TaskUpdateFunction): Promise<Error | null> {
    this.state = TaskState.Running;
    try {
      await this.executionFunction(fStatus);
      this.state = TaskState.Done;
      return null;
    } catch (error) {
      this.state = TaskState.Error;
      return error as Error;
    }
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
  private readonly status: StatusManager<number> | null;

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
   * Sempahore for the ready queue.
   */
  private readySemaphore = new Semaphore(0)

  private _error: Error | null = null;

  constructor(public readonly config: TaskRunnerConstructor<Tags>) {
    this.concurrencyLevel = config.concurrencyLevel ?? getNumCpus().length;
    this.status = config.showStatus ? new StatusManager() : null
  }

  /**
   * Enqueues a task to run.
   */
  addTask(config: TaskConstructor<Tags>, executionFunction: (fStatus: TaskUpdateFunction) => Promise<void>): Task<Tags> {
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
  private updateStatus(statusIdx: number, msg: string) {
    if (this.status) {
      this.status.update(statusIdx, msg)
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
    this.readySemaphore.release()
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
      console.log(`Jobs finished; pid=${process.pid}; ${this.numTasksCompleted} tasks completed in ${Math.ceil((tEnd - tStart) / 1000)}s`);
    }
  }

  /**
   * Runs one worker until queues are finished.
   */
  private async worker(statusIdx: number): Promise<void> {
    let hasDoneAnything = false   // don't emit messages until we've actually done something, so we don't take a slot on the command-line
    // Our own status function that only updates status if we've done something, and uses our worker index as a key
    const fStatus = (msg: string) => (hasDoneAnything && this.updateStatus(statusIdx, msg))
    while (this._error === null && (this.readyQueue.length > 0 || this.waitingQueue.length > 0 || this.runningTasks.length > 0)) {

      try {
        // Wait to acquire the semaphore, which means something is ready to run.
        if (this.readySemaphore.isLocked()) {
          // If we believe we'll be waiting, it's worth updating the status.
          // Otherwise, we're about to run something, so don't bother flickering the screen.
          fStatus("💤")
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
      fStatus(`🏃‍♂️ ${task.title}`)
      this.runningTasks.push(task);
      TaskRunner.updateTagCounter(task, this.numQueuedTasksByTag, -1)
      TaskRunner.updateTagCounter(task, this.numRunningTasksByTag, 1)

      // Execute the task
      const tStart = Date.now()
      const error = await task.execute((msg: string) => this.updateStatus(statusIdx, `🏃‍♂️ ${task.title}: ${msg}`));
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
    fStatus("✌️")

    // Got here because we're totally done.
    // Some workers might still be going; that's fine.
    // Wake the remaining ones so they can exit normally
    this.readySemaphore.cancel()
  }

  /**
   * The first error encountered, if any.
   */
  get error(): Error | null {
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

//   for (let i = 0; i < 35; ++i) {
//     const tagged = i % 2 == 1
//     manager.addTask({
//       title: `Task ${i}`,
//       tags: tagged ? ["foo"] : ["bar"],
//       dependentTags: tagged ? [] : ["foo"],
//       dependentTasks: [mainTask],
//     }, async (fStatus) => {
//       fStatus("Wait A")
//       await new Promise(resolve => setTimeout(resolve, Math.random() * 500));
//       fStatus("Wait B")
//       await new Promise(resolve => setTimeout(resolve, Math.random() * 500));
//       fStatus("Wait C")
//       await new Promise(resolve => setTimeout(resolve, Math.random() * 500));
//       fStatus("Done")
//     });
//   }

//   await manager.run();

//   if (manager.error) {
//     console.error("An error occurred:", manager.error);
//   } else {
//     console.log("All tasks completed successfully");
//   }
// }

// main().catch(console.error);