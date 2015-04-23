
# Datapipe

[![Build Status](https://travis-ci.org/ibab/datapipe.svg?branch=master)](https://travis-ci.org/ibab/datapipe)

Datapipe is a Python framework that allows you to build and manage complex data processing pipelines.

## Why not use existing data processing frameworks? 

Datapipe is inspired by similar packages like [luigi](https://github.com/spotify/luigi) and [ruffus](http://www.ruffus.org.uk/).
It aims to improve on alternatives by

 - providing an API that makes tasks fully *composable* 
   - Tasks can have arbitrary inputs/outputs
   - Tasks can be defined separately from the data processing pipeline
   - Tasks can be combined dynamically (any output of a task can become the input of another one)
 - speeding up complex, repetitive workflows
   - Parallel execution of tasks
   - Only rerun a task if necessary or requested (like `make`)

## Current features

 - A flexible API for defining tasks, inspired by (but different from) [luigi](https://github.com/spotify/luigi)
 - The state of targets is tracked and only necessary tasks are performed (e.g. when the timestamp of a file changes, the code of a task is changed, or the structure of the pipeline is modified)
 - Implemented targets:
   - `LocalFile` (a file on the local filesystem)
   - `PyTarget` (a Python object that will be automatically persisted)

## Planned features

 - Various base tasks and targets for working with
   - remote files (e.g. via `ssh`)
   - shell commands
   - batch schedulers (PBS/SLURM/…)
   - Apache Hadoop/Spark
   - version control systems
   - compilers
 - Interactive web UI that allows you to monitor, start and stop tasks
   (and possibly even restructure the pipeline and implement new tasks)

## How are changes tracked?

Datapipe stores its targets in a [LevelDB](http://leveldb.org/) key-value store.
This allows targets to compare themselves to a previous version and decide if they are up to date.

## Example

```python
from datapipe import Task, Input, LocalFile, require

# New tasks are defined by inheriting from an existing Task
class AddLines(Task):

    # The inputs can be anything the task depends on:
    # Local and remote files, python objects, numpy arrays, ...
    # These are implemented as subclasses of Target
    infile = Input()
    count = Input(default=1)
    text = Input(default='This is some text')

    # The outputs are defined dynamically (with access to the inputs)
    def outputs(self):
        return LocalFile(self.infile.path().replace('.txt', '.AddLines.txt'))

    # The actual task is defined as a function with access to inputs and outputs
    def run(self):
        with open(self.infile.path()) as f:
            with open(self.outputs().path(), 'w') as g:
                g.write(f.read())
                for i in range(self.count):
                    g.write(self.text + '\n')

# Create initial targets
infile = LocalFile('input.txt')

# Define the pipeline
target1 = AddLines(infile, count=2).outputs()
target2 = AddLines(target1, count=3, text='This is some more text').outputs()

# Require a target to execute all tasks needed to produce it
require(target2)
```

The log output for the above example looks like this:
```
INFO     REQUIRE LocalFile('input.AddLines.AddLines.txt')
INFO     RUNNING AddLines(infile=LocalFile('input.txt'), count=2, text=This is some text)
INFO     FINISHED AddLines
INFO     RUNNING AddLines(infile=LocalFile('input.AddLines.txt'), count=3, text=This is some more text)
INFO     FINISHED AddLines
INFO     DONE LocalFile('input.AddLines.AddLines.txt')
```

On the next run, the targets are already up to date and all tasks are skipped:
```
INFO     REQUIRE LocalFile('input.AddLines.AddLines.txt')
INFO     DONE LocalFile('input.AddLines.AddLines.txt')
```

