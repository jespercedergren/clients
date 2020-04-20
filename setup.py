from setuptools import setup

setup(
   name='clients',
   version='0.1',
   author='Jesper Cedergren',
   author_email='jesper.cedergren@hotmail.com',
   packages=['clients'],  #same as name
   install_requires=['s3fs==0.4.2', 'fastparquet==0.3.3', 'toolz==0.10.0', 'dask==2.14.0', 'pyarrow==0.16.0']
   #external packages as dependencies
)