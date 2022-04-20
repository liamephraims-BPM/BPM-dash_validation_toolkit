import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='BPM-dash_validation_toolkit',
    version='0.20.01',
    author='Liam Ephraims',
    author_email='liam.ephraims@bigpicturemedical.com',
    description='Use driver functions and utility functions to run stage 1, 2 and 3 checks, can also run individual checks',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/liamephraims-BPM/BPM-dash_validation_toolkit',
    license='MIT',
    packages=['BPM-dash_validation_toolkit'],
    install_requires=['pyathena', 'pandas'] #, 
)
