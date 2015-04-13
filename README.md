
# Datapipe

![build status](https://travis-ci.org/ibab/datapipe.svg)

Datapipe is a Python framework that allows you to build and manage complex data processing pipelines.

## Why not use existing data processing frameworks? 

Datapipe is inspired by similar packages like [luigi](https://github.com/spotify/luigi).
It aims to improve on alternatives by

 - providing an API that makes tasks fully *composable* 
   - Tasks are defined separately from the data processing pipeline
   - can be combined dynamically
 - speeding up complex, repetitive workflows
   - Parallel execution of tasks
   - Only rerun a task if necessary or requested (like `make`)

## Planned features

 - Anything can be an input/output of a task (local/remote files, python objects, …) and will be tracked by datapipe
 - Various base tasks and targets for working with
   - local files
   - remote files (e.g. via `ssh`)
   - shell commands
   - batch schedulers (PBS/SLURM/…)
   - Apache Hadoop/Spark
   - version control systems
   - compilers
 - Interactive web UI that allows you to monitor, start and stop tasks
   (and possibly even restructure the pipeline and implement new tasks)

## Example

```python
from datapipe import Task, Input, LocalFile, require

# New tasks are defined by inheriting from an existing Task
class AddLines(Task):

    # The inputs can be anything the task depends on:
    # Local and remote files, python objects, numpy arrays, ...
    infile = Input()
    count = Input(default=1)
    text = Input(default='This is some text')

    # The outputs are defined dynamically (with access to the inputs)
    def outputs(self):
        return LocalFile(self.infile.get().replace('.txt', '.AddLines.txt'))

    # The actual task is defined as a function with access to inputs and outputs
    def run(self):
        with open(self.infile.get()) as f:
            with open(self.outputs().get(), 'w') as g:
                g.write(f.read())
                for i in range(self.count):
                    g.write(self.text + '\n')

# Create initial Targets
infile = LocalFile('input.txt')

# Define the pipeline
task1 = AddLines(infile, count=2)
task2 = AddLines(task1.outputs(), count=3, text='This is some more text')

# Require a target to execute all tasks needed to produce it
require(task2.outputs())
```

The log output for the above example looks like this:
```
INFO - REQUIRE LocalFile('input.AddLines.AddLines.txt')
INFO - RUNNING AddLines(infile=LocalFile('input.txt'), count=2, text='This is some text')
INFO - FINISHED AddLines(infile=LocalFile('input.txt'), count=2, text='This is some text')
INFO - RUNNING AddLines(infile=LocalFile('input.AddLines.txt'), count=3, text='This is some more text')
INFO - FINISHED AddLines(infile=LocalFile('input.AddLines.txt'), count=3, text='This is some more text')
```

On subsequent runs, tasks whose outputs are up to date are skipped:
```
INFO - REQUIRE LocalFile('input.AddLines.AddLines.txt')
INFO - SKIPPING AddLines(infile=LocalFile('input.txt'), count=2, text='This is some text')
INFO - SKIPPING AddLines(infile=LocalFile('input.AddLines.txt'), count=3, text='This is some more text')
```

