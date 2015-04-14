
from setuptools import setup

setup(name='datapipe',
      version='0.0.0',
      description='Flexible data processing framework',
      url='http://github.com/ibab/datapipe',
      author='Igor Babuschkin',
      author_email='igor@babuschk.in',
      license='MIT',
      packages=['datapipe'],
      install_requires=[
          'six',
          'dask',
          'joblib',
      ],
      zip_safe=False)
