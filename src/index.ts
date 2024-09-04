import { cpus as getNumCpus } from 'os';

/**
 * Task execution state.  Goes from `New` to `Running` to `Done` or `Error`.
 */
export enum TaskState {
  New,
  Running,
  Done,
  Error
}

export type TaskRunnerConstructor = {

  /**
   * The concurrency level, or undefined to use the number of CPUs available.
   */
  concurrencyLevel?: number,

  /**
   * Log on the console when the whole runner starts?
   */
  logRunnerStart?: boolean

  /**
   * Log on the console when the whole runner completes?
   */
  logRunnerEnd?: boolean

  /**
   * Log on the console when a task begins?
   */
  logTaskStart?: boolean

  /**
   * Log on the console when a task completes?
   */
  logTaskEnd?: boolean
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
   * Tasks will run in priority-order, from lowest to highest, though different priorities can be running concurrently.
   */

  priority?: number,

  /**
   * Don't start this task unless all tasks with a lower completionPriority have not only started, but finished.
   */
  completionPriority?: number,

  /**
   * Tags to apply to this task.
   */
  tags?: Tags[]
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
   * Tasks will run in priority-order, from lowest to highest, though different priorities can be running concurrently.
   */
  public readonly priority: number

  /**
   * Don't start this task unless all tasks with a lower completionPriority have not only started, but finished.
   */
  public readonly completionPriority: number

  /**
   * Tags that are applied to this task.
   */
  public readonly tags: Tags[]

  /**
   * The current state of the Task.
   */
  public state: TaskState = TaskState.New;
  private readonly dependentTasks: Task<Tags>[] = [];

  constructor(config: TaskConstructor<Tags>, private readonly executionFunction: () => Promise<void>) {
    this.title = config.title
    this.priority = config.priority ?? 0
    this.completionPriority = config.completionPriority ?? 0
    this.tags = config.tags ?? []
    if (this.tags.length > 0) {
      this.title += " [" + this.tags.join(", ") + "]"
    }
  }

  isReady(readyQueue: Task<Tags>[], runningTasks: Task<Tags>[]): boolean {
    if (this.state !== TaskState.New) return false;

    // Check task list dependency
    if (this.dependentTasks.some(task => task.state !== TaskState.Done)) return false;

    // Check weight dependency against ready queue
    if (readyQueue.some(task => task.priority < this.priority || task.priority < this.completionPriority)) return false;

    // Check completion weight dependency against running tasks
    if (runningTasks.some(task => task.priority < this.completionPriority)) return false;

    return true;
  }

  /**
   * Executes this Task, returning any `Error` thrown, or `null` if nothing was thrown.
   */
  async execute(): Promise<Error | null> {
    this.state = TaskState.Running;
    try {
      await this.executionFunction();
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
  private queue: Task<Tags>[] = [];
  private runningTasks: Task<Tags>[] = [];
  private doneTasks: Task<Tags>[] = [];
  private _error: Error | null = null;

  constructor(public readonly config: TaskRunnerConstructor) {
    // nothing else to do
  }

  /**
   * Enqueues a task to run.
   */
  addTask<TheseTags extends Tags>(config: TaskConstructor<TheseTags>, executionFunction: () => Promise<void>): Task<TheseTags> {
    const task = new Task(config, executionFunction)
    this.queue.push(task);
    return task
  }

  /**
   * Runs all tasks until completion.  Tasks can add more tasks.
   * Once finished, check `this.error` for whether there were problems.
   */
  async run(): Promise<void> {
    const tStart = Date.now();
    const concurrencyLevel = this.config.concurrencyLevel ?? getNumCpus().length;
    if (this.config.logRunnerStart) {
      console.log(`Jobs starting; pid=${process.pid}; concurrency=${concurrencyLevel}`);
    }
    const workers = Array(concurrencyLevel).fill(null).map(() => this.worker());
    await Promise.all(workers);
    if (this.config.logRunnerEnd) {
      console.log(`Jobs finished; pid=${process.pid}; ${this.doneTasks.length} tasks completed in ${Math.ceil((Date.now() - tStart) / 1000)}s`);
    }
  }

  /**
   * Runs one worker until queues are finished.
   */
  private async worker(): Promise<void> {
    while (this._error === null && this.queue.length > 0) {

      // Find the next task to run
      const readyTaskIndex = this.queue.findIndex(task => task.isReady(this.queue, this.runningTasks));
      if (readyTaskIndex === -1) {
        await new Promise(resolve => setTimeout(resolve, 100)); // Wait a beat
        continue;
      }

      // Run the task
      const task = this.queue.splice(readyTaskIndex, 1)[0];
      if (this.config.logTaskStart) {
        console.log(`>>> ${task.title}`)
      }
      this.runningTasks.push(task);
      const tStart = Date.now()
      const error = await task.execute();
      const tDuration = Date.now() - tStart
      if (this.config.logTaskEnd) {
        console.log(`<<< ${task.title} in ${tDuration}ms ${error ? ` (ERR: ${error.message})` : ""}`);
      }
      this.runningTasks = this.runningTasks.filter(t => t !== task);
      this.doneTasks.push(task);
      if (error) {
        this._error = error;
      }
    }
  }

  /**
   * The first error encountered, if any.
   */
  get error(): Error | null {
    return this._error;
  }
}

// Usage example
async function main() {
  const manager = new TaskRunner<"foo" | "bar">({
    concurrencyLevel: 2,
    logRunnerStart: true,
    logRunnerEnd: true,
    logTaskStart: true,
    logTaskEnd: true,
  });

  const task1 = manager.addTask({
    title: "Task 1",
    priority: 1,
    tags: ["foo"],
  }, async () => {
    await new Promise(resolve => setTimeout(resolve, 500));
  });

  const task2 = manager.addTask({
    title: "Task 2",
    priority: 2,
    tags: ["foo"],
  }, async () => {
    await new Promise(resolve => setTimeout(resolve, 750));
    // throw new Error("Task 2 failed");
  });

  const task3 = manager.addTask({
    title: "Task 3",
    priority: 3,
  }, async () => {
    await new Promise(resolve => setTimeout(resolve, 250));
  });

  const task4 = manager.addTask({
    title: "Task 4",
    priority: 4,
  }, async () => {
    await new Promise(resolve => setTimeout(resolve, 1));
  });

  // task1.addDependentTask(task3);
  // task2.addDependentTask(task3);

  await manager.run();

  if (manager.error) {
    console.error("An error occurred:", manager.error);
  } else {
    console.log("All tasks completed successfully");
  }
}

main().catch(console.error);