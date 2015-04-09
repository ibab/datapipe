
from datapipe import Task, Input, LocalFile

class AddLines(Task):

    infile = Input()
    count = Input(default=1)
    text = Input(default='This is a test')

    def output(self):
        return self.infile.from_suffix('.txt', '.AddLines.txt')

    def run(self):
        with open(self.infile.get()) as f:
            with open(self.output().get(), 'w') as g:
                g.write(f.read())
                for i in range(self.count):
                    g.write('This is a test\n')


infile = LocalFile('out.txt')

task1 = AddLines(infile, count=2)
task2 = AddLines(task1.output(), count=3, text='This is another test')

task1.run()
task2.run()

