from setuptools import setup, Extension, find_packages
from Cython.Distutils import build_ext
from Cython.Build import cythonize
import sys
import os

# Determine whether to use Cython
if '--cythonize' in sys.argv:
    cythonize_switch = True
    del sys.argv[sys.argv.index('--cythonize')]
    ext = 'pyx'
else:
    cythonize_switch = False
    ext = 'c'

ext_modules = [
#                Extension('oreader.base', 
#                          [os.path.join('oreader', 'base.%s' % ext)]),
               ]

setup(name='oreader',
      version='0.1',
      cmdclass = {'build_ext': build_ext},
      description='Tools for fast ORM style reading and writing of hierarchical data',
      author='Jason Rudy',
      author_email='jcrudy@gmail.com',
      url='https://github.com/jcrudy/oreader',
      packages=['oreader'],
      ext_modules = cythonize(ext_modules) if cythonize_switch else ext_modules,
      requires=['cython']
     )