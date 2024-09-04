# Jobs

Simple job-runner with features:

* Max simultaneous threads, but also max-per-tag
* Priority queue for job-start ordering
* Priority queue for job-start-after-others-complete ordering
* Tasks dependent on specific other tasks being complete

## Execution queue

* All else being equal, tasks are started in the order they were enqueued.
* Tasks will not start based on:
    * max running tasks per tag
    * priority queue for others not started
    * priority queue for others others not completed
    * dependency on specific other tasks
* Tasks that have to wait maintain their position in the queue.
* Any error stops kicking off new tasks, allows existing tasks to complete, and the error is available.

## Development Usage

Build:

```bash
npm run build
```

Unit tests:

```bash
npm run test
```

Unit tests, refreshed live:

```bash
npm run watch
```

Prepare for release (e.g. run tests and bump version number):

```bash
npm run release
```

Publish to npm:

```bash
npm publish
```
