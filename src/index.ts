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

/**
 * Options when creating a new `Task`.
 */
export type TaskConstructor = {
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
}

/**
 * Runnable Task.
 */
export class Task {
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
   * The current state of the Task.
   */
  public state: TaskState = TaskState.New;
  private readonly dependentTasks: Task[] = [];

  constructor(config: TaskConstructor, private readonly executionFunction: () => Promise<void>) {
    this.title = config.title
    this.priority = config.priority ?? 0
    this.completionPriority = config.completionPriority ?? 0
  }

  isReady(readyQueue: Task[], runningTasks: Task[]): boolean {
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
  addDependentTask(task: Task): void {
    this.dependentTasks.push(task);
  }
}

/**
 * A Task-running system.  Load initial tasks; tasks can beget other tasks.  Can run until all are complete.
 */
export class ExecutionManager {
  private queue: Task[] = [];
  private runningTasks: Task[] = [];
  private doneTasks: Task[] = [];
  private _error: Error | null = null;
  private concurrencyLevel: number;

  constructor(concurrencyLevel: number, public readonly verbose: boolean) {
    this.concurrencyLevel = concurrencyLevel <= 0 ? getNumCpus().length : concurrencyLevel;
  }

  /**
   * Enqueues a task to run.
   */
  addTask(task: Task): void {
    this.queue.push(task);
  }

  /**
   * Runs all tasks until completion.  Tasks can add more tasks.
   * Once finished, check `this.error` for whether there were problems.
   */
  async run(): Promise<void> {
    const tStart = Date.now();
    if (this.verbose) {
      console.log(`Jobs starting; pid=${process.pid}; concurrency=${this.concurrencyLevel}`);
    }
    const workers = Array(this.concurrencyLevel).fill(null).map(() => this.worker());
    await Promise.all(workers);
    if (this.verbose) {
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
      if (this.verbose) {
        // console.log(`>>> ${task.title}`)
      }
      this.runningTasks.push(task);
      const tStart = Date.now()
      const error = await task.execute();
      const tDuration = Date.now() - tStart
      if (this.verbose) {
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
// async function main() {
//   const manager = new ExecutionManager(3, true);

//   const task1 = new Task({
//     title: "Task 1"
//   }, async () => {
//     await new Promise(resolve => setTimeout(resolve, 1000));
//   });

//   const task2 = new Task({
//     title: "Task 2"
//   }, async () => {
//     await new Promise(resolve => setTimeout(resolve, 1500));
//     // throw new Error("Task 2 failed");
//   });

//   const task3 = new Task({
//     title: "Task 3"
//   }, async () => {
//     await new Promise(resolve => setTimeout(resolve, 500));
//   });

//   // task1.addDependentTask(task3);
//   // task2.addDependentTask(task3);

//   manager.addTask(task1);
//   manager.addTask(task2);
//   manager.addTask(task3);

//   await manager.run();

//   if (manager.error) {
//     console.error("An error occurred:", manager.error);
//   } else {
//     console.log("All tasks completed successfully");
//   }
// }

// main().catch(console.error);