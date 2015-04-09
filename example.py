
from datapipe import Task, Input, LocalFile, require

# New tasks are defined by inheriting from an existing Task
class AddLines(Task):

    # The inputs can be anything that inherits from Target
    # or that can be converted to a target: local and remote
    # files, python objects, numpy arrays, ...
    infile = Input()
    count = Input(default=1)
    text = Input(default='This is some text')

    # The outputs are defined dynamically (with access to the inputs)
    def outputs(self):
        return self.infile.from_suffix('.txt', '.AddLines.txt')

    # The actual task is defined as a function with access to inputs and outputs
    def run(self):
        with open(self.infile.get()) as f:
            with open(self.outputs().get(), 'w') as g:
                g.write(f.read())
                for i in range(self.count):
                    g.write(self.text + '\n')

# Create initial Targets
infile = LocalFile('out.txt')

# Define the pipeline
task1 = AddLines(infile, count=2)
task2 = AddLines(task1.outputs(), count=3, text='This is some more text')

# Require any target to execute the necessary tasks
require(task2.outputs())

