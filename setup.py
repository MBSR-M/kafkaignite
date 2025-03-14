#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from setuptools import setup, find_packages
import datetime

from utils.use_version import version

setup(
    name='wheel-name',
    version=version(),
    description='Description',
    author='Mubashir',
    author_email='mubashir.ai@outlook.com',
    include_package_data=True,
    package_dir={'': 'src'},
    package_data={'': ['*.json', '*.yml', '*.conf']},
    license='Other/Proprietary License',
    install_requires=[
        'kafka-python',
        'pymysql',
        'mysql-connector-python',
        'python-dateutil',
        'requests',
        'pytz',
        'openpyxl',
        'pandas',
        'cryptography',
    ],
    extras_require={
        'test': [
            'pytest',
            'pytest-cov',
        ]
    },
    entry_points={
        'console_scripts': [

        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: Other/Proprietary License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.10',
)
