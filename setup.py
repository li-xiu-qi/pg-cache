from setuptools import setup, find_packages

setup(
    name='pg-cache',
    version='0.1.6.1',
    description='A PostgreSQL wrapper similar to diskcache',
    long_description=open('README.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/li-xiu-qi/pg-cache',
    author='li-xiu-qi',
    author_email='lixiuqixiaoke@qq.com',
    packages=find_packages(),
    install_requires=[
        'sqlalchemy>=1.4',
        'asyncpg>=0.21'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)