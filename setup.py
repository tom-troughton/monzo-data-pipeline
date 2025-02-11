from setuptools import setup, find_packages

setup(
    name="monzo_utils",
    version="0.1.0",
    packages=find_packages(
        exclude=[
            'src.*',
            'new_src.*'
        ]
    ),
    install_requires=[],
)