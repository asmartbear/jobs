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
  public readonly dependentTags: Tags[]

  private readonly dependentTasks: Task<Tags>[] = [];

  /**
   * The current state of the Task.
   */
  public state: TaskState = TaskState.New;

  constructor(config: TaskConstructor<Tags>, private readonly executionFunction: (fStatus: TaskUpdateFunction) => Promise<void>) {
    this.title = config.title
    this.tags = config.tags ?? []
    this.dependentTags = config.dependentTags ?? []
    if (this.tags.length > 0) {
      this.title += " [" + this.tags.join(", ") + "]"
    }
  }

  isReady(config: TaskRunnerConstructor<Tags>, numQueuedTasksByTag: Map<Tags, number>, numRunningTasksByTag: Map<Tags, number>): boolean {

    // Have to be "new" to be ready
    if (this.state !== TaskState.New) return false;

    // Check task list dependency
    if (this.dependentTasks.some(task => task.state !== TaskState.Done)) return false;

    // Check weight dependency against ready queue
    // if (readyQueue.some(task => task.priority < this.priority || task.priority < this.completionPriority)) return false;

    // Check completion weight dependency against running tasks
    // if (runningTasks.some(task => task.priority < this.completionPriority)) return false;

    // Check tag-based completion dependency
    if (this.dependentTags.some(tag =>
      (numQueuedTasksByTag.get(tag) ?? 0) + (numRunningTasksByTag.get(tag) ?? 0) > 0
    )) return false;

    // Check concurrency-based tags
    for (const tag of this.tags) {
      const concurrency = config.concurrencyPerTag?.[tag] ?? 9999;
      const current = numRunningTasksByTag.get(tag) ?? 0
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
  private queue: Task<Tags>[] = [];
  private runningTasks: Task<Tags>[] = [];
  private _error: Error | null = null;
  private numTasksCompleted = 0
  private numQueuedTasksByTag = new Map<Tags, number>()
  private numRunningTasksByTag = new Map<Tags, number>()

  constructor(public readonly config: TaskRunnerConstructor<Tags>) {
    this.concurrencyLevel = config.concurrencyLevel ?? getNumCpus().length;
    this.status = config.showStatus ? new StatusManager() : null
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
   * Enqueues a task to run.
   */
  addTask<TheseTags extends Tags>(config: TaskConstructor<TheseTags>, executionFunction: (fStatus: TaskUpdateFunction) => Promise<void>): Task<TheseTags> {
    const task = new Task(config, executionFunction)
    this.queue.push(task);
    TaskRunner.updateTagCounter(task, this.numQueuedTasksByTag, 1)
    return task
  }

  /**
   * Runs all tasks until completion.  Tasks can add more tasks.
   * Once finished, check `this.error` for whether there were problems.
   */
  async run(): Promise<void> {
    const tStart = Date.now();
    if (this.status) {
      console.log(`Jobs starting; pid=${process.pid}; concurrency=${this.concurrencyLevel}`);
      this.status.start()
    }
    const workers = Array(this.concurrencyLevel).fill(null).map((_, i) => this.worker(i));
    await Promise.all(workers);
    if (this.status) {
      this.status.stop()
      console.log(`Jobs finished; pid=${process.pid}; ${this.numTasksCompleted} tasks completed in ${Math.ceil((Date.now() - tStart) / 1000)}s`);
    }
  }

  /**
   * Runs one worker until queues are finished.
   */
  private async worker(statusIdx: number): Promise<void> {
    let hasDoneAnything = false   // don't emit messages until we've actually done something, so we don't take a slot on the command-line
    while (this._error === null && (this.queue.length > 0 || this.runningTasks.length > 0)) {

      // Find the next runnable task
      const readyTaskIndex = this.queue.findIndex(task => task.isReady(this.config, this.numQueuedTasksByTag, this.numRunningTasksByTag));
      if (readyTaskIndex === -1) {
        // console.log("wait")
        if (hasDoneAnything) this.updateStatus(statusIdx, "💤")
        await new Promise(resolve => setTimeout(resolve, 50)); // Wait a beat
        continue;
      }

      // Run the task
      hasDoneAnything = true
      const task = this.queue.splice(readyTaskIndex, 1)[0];
      this.updateStatus(statusIdx, `🏃‍♂️ ${task.title}`)
      this.runningTasks.push(task);
      TaskRunner.updateTagCounter(task, this.numQueuedTasksByTag, -1)
      TaskRunner.updateTagCounter(task, this.numRunningTasksByTag, 1)
      const tStart = Date.now()
      const error = await task.execute((msg: string) => this.updateStatus(statusIdx, `🏃‍♂️ ${task.title}: ${msg}`));
      const tDuration = Date.now() - tStart

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

      // Remove from running list
      this.runningTasks = this.runningTasks.filter(t => t !== task);
      TaskRunner.updateTagCounter(task, this.numRunningTasksByTag, -1)
      ++this.numTasksCompleted
      if (error) {
        this._error = error;
      }
    }
    if (hasDoneAnything) this.updateStatus(statusIdx, "✌️")
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

//   for (let i = 0; i < 35; ++i) {
//     const tagged = i % 2 == 1
//     manager.addTask({
//       title: `Task ${i}`,
//       tags: tagged ? ["foo"] : ["bar"],
//       dependentTags: tagged ? [] : ["foo"],
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