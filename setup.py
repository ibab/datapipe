
from setuptools import setup

setup(name='datapipe',
      version='0.0.0',
      description='Flexible data processing framework',
      url='http://github.com/ibab/datapipe',
      author='Igor Babuschkin',
      author_email='igor@babuschk.in',
      license='MIT',
      packages=['datapipe', 'datapipe.targets'],
      install_requires=[
          'six',
          'dask',
          'joblib',
          'dill',
          'leveldb',
      ],
      zip_safe=False)
