import setuptools

with open("README.md", "r", encoding = "utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name = "package-name",
    version = "0.0.1",
    author = "Eugeniu Ursu",
    author_email = "eugeniu.ursu@gmail.com",
    description = "Python Data Processing Tools",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "https://github.com/egursu/unidata",
    classifiers = [
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir = {"": "src"},
    packages = setuptools.find_packages(where="src"),
    python_requires = ">=3.9"
)