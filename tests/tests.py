import logging
logger = logging.getLogger('datapipe')
logger.setLevel(logging.WARN)

from datapipe import *
from datapipe.targets.mock import *

class TestTask(Task):
    inp = Input()

    def outputs(self):
        outs = []
        for i, elem in enumerate(self.inp):
            outs.append(MockTarget('out_{}'.format(i)))
        return outs
    
    def run(self):
        pass

# New tasks are defined by inheriting from an existing Task
class AddLines(Task):

    # The inputs can be anything the task depends on:
    # Local and remote files, python objects, numpy arrays, ...
    infile = Input()
    count = Input(default=1)
    text = Input(default='This is some text')

    # The outputs are defined dynamically (with access to the inputs)
    def outputs(self):
        return LocalFile(self.infile.path().replace('.txt', '.AddLines.txt'))

    # The actual task is defined as a function with access to inputs and outputs
    def run(self):
        with self.infile.open() as f:
            with self.outputs().open('w') as g:
                g.write(f.read())
                for i in range(self.count):
                    g.write(self.text + '\n')

def test_simpletask():
    target = MockTarget('test1')
    TestTask([target]).run()

def test_several_targets():
    inputs = [MockTarget('input_1'), MockTarget('input_2')]
    task = TestTask(inputs)
    task.run()

def test_example():

    # Create test file
    open('/tmp/input.txt', 'w').close()

    from datapipe import Task, Input, LocalFile, require


    # Create initial Targets
    infile = LocalFile('/tmp/input.txt')

    # Define the pipeline
    task1 = AddLines(infile, count=2)
    task2 = AddLines(task1.outputs(), count=3, text='This is some more text')

    # Require a target to execute all tasks needed to produce it
    require(task2.outputs())


