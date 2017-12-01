from setuptools import setup, find_packages
import versioneer


setup(name='oreader',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Tools for fast ORM style reading and writing of hierarchical data',
      author='Jason Rudy',
      author_email='jcrudy@gmail.com',
      url='https://github.com/jcrudy/oreader',
      packages=find_packages(),
      install_requires=['sqlalchemy', 'pandas', 'interval', 'frozendict', 'arrow', 'cyinterval'],
      tests_require=['names', 'nose', 'infinity', 'toolz']
     )