
from datapipe import Task, Input, FilePath

class AddLine(Task):

    infile = Input()
    count = Input(default=1)

    def output(self):
        return self.infile.from_suffix('.txt', '.processed.txt')

    def run(self):
        with open(self.infile.get()) as f:
            with open(self.output().get(), 'w') as g:
                g.write(f.read())
                for i in range(self.count):
                    g.write('This is a test\n')


infile = FilePath('out.txt')

task = AddLine(infile, count=2)

task.run()

