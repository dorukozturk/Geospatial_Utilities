from setuptools import setup

setup(name='geoutils',
      version='0.0',
      description='Simple utilities for working with geospatial data',
      url='https://github.com/dorukozturk/Geospatial_Utilities',
      author='Doruk Ozturk',
      author_email='doruk.ozturk@kitware.com',
      license='Apache 2.0',
      packages=['geoutils'],
      zip_safe=False,
      install_requires=[
          'boto',
          'ansible',
          'celery',
          'requests',
          'lxml'],
      entry_points={
          'console_scripts': [
              "hdf2tiff=geoutils.hdf2tiff:main"
          ]
      }
)
