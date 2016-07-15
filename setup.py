from setuptools import setup, find_packages
import dsq

setup(
    name='dsq',
    version=dsq.version,
    url='https://github.com/baverman/dsq/',
    license='MIT',
    author='Anton Bobrov',
    author_email='baverman@gmail.com',
    description='Dead simple task queue using redis',
    long_description=open('README.rst').read(),
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=[
        'redis >= 2.7.0',
        'click >= 5.0.0',
        'msgpack-python>=0.4.0',
    ],
    entry_points={
        'console_scripts': ['dsq = dsq.cli:cli']
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        # 'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Systems Administration',
        'Topic :: System :: Monitoring',
    ]
)
