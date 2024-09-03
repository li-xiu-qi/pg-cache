from setuptools import setup, find_packages

setup(
    name='pg-cache',
    version='0.1.1',
    description='A postgresql wrapper similar to diskcache',
    long_description=open('README.md').read(),
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