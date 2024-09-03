from setuptools import setup, find_packages

setup(
    name='pg_cache',
    version='0.1.0',
    description='A caching package with both synchronous and asynchronous support for PostgreSQL.',
    long_description=open('README_zh.md').read(),
    long_description_content_type='text/markdown',
    author='Your Name',
    author_email='your.email@example.com',
    url='https://github.com/li-xiu-qi/pg_cache',
    packages=find_packages(),
    install_requires=[
        'sqlalchemy',
        'asyncpg',
        'psycopg2-binary'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)