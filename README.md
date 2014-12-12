
# Datapipe

Datapipe is a planned Python framework for processing data:

 - Modularize individual steps of your pipeline into composable *tasks*
 - Compose tasks using a flexible syntax inspired by functional programming
 - Only run those tasks whose inputs have changed
 - Automatically generate a command line interface for driving your pipeline
 - Graphically display your data flow
 - Monitor and run the pipeline from a web interface
 - Possible integration with IPython notebook?

Benefits of using Datapipe:
 
 - Keeps track of which tasks have been applied to which datasets so you don't have to
 - Makes your data analysis fully reproducible
 - Modular and composable tasks allow you to work effectively with others

Inspirations: [ruffus](http://code.google.com/p/ruffus/), [luigi](https://github.com/spotify/luigi)

## Mockup

```python
from datapipe import DataPipe

pipe = DataPipe()

@pipe.task
def processA(infile, outfile):
    # add something to infile

@pipe.task
def processB(infile, outfile):
    # add something to infile

@pipe.task
def mergeAB(infiles, outfile):
    # merge all infiles to outfile

fs = pipe.load(['test.txt'])

outputs = pipe.map([processA, processB], fs)
pipe.zip(mergeAB, outputs)

pipe.run()

```

