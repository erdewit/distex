import os
import sys
import codecs
from setuptools import setup

if sys.version_info < (3, 0, 0):
    raise RuntimeError("distex is for Python 3")

here = os.path.abspath(os.path.dirname(__file__))
with codecs.open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='distex',
    version='0.5.4',
    description='Async distributed process pool using asyncio',
    long_description=long_description,
    url='https://github.com/erdewit/distex',
    author='Ewald R. de Wit',
    author_email='ewald.de.wit@gmail.com',
    license='BSD',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3 :: Only',
    ],
    keywords='python asyncio parallel distributed computing process pool task queue',
    packages=['distex'],
    install_requires=['dill', 'cloudpickle'],
)
