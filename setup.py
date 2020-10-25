import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Clearmatch",
    version="0.0.1",
    author="Darren Colby",
    author_email="dscolby17@gmail.com",
    description="Library for deterministic record matching",
    long_description="Clearmatch matches records from one dataset to another by using a key, which has reference"
                     "records. If the records to be matched to are synonyms of a reference record in the key, "
                     "that record is matched with its reference. Clearmatch also makes it easy to see summary "
                     "statistics and generate bar plots of missingness",
    long_description_content_type="text/markdown",
    url="https://github.com/dscolby/clearmatch",
    packages=['clearmatch', 'test'],
    install_requires=['matplotlib', 'pandas', 'numpy'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
