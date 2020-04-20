from setuptools import setup

setup(
   name='clients',
   version='0.1',
   author='Jesper Cedergren',
   author_email='jesper.cedergren@hotmail.com',
   packages=['clients'],  #same as name
   install_requires=['s3fs', 'fastparquet', 'toolz', 'dask', 'pyarrow'] #external packages as dependencies
)