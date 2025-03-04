from setuptools import setup, find_packages

# Get version from version.py
with open("sparvi/version.py", "r") as f:
    exec(f.read())

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="sparvi-core",
    version=__version__,
    description="Data profiling and validation engine for modern data warehouses",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Sparvi Team",
    author_email="info@sparvi.io",
    url="https://github.com/sparvi-io/sparvi-core",
    packages=find_packages(include=["sparvi", "sparvi.*"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=[
        "sqlalchemy>=1.4.0",
        "pandas>=1.3.0",
        "numpy>=1.20.0",
        "click>=8.0.0",
        "pyyaml>=6.0",
        "rich>=10.0.0",
        "duckdb>=1.0.0",
    ],
    extras_require={
        "snowflake": ["snowflake-sqlalchemy"],
        "postgres": ["psycopg2-binary"],
        "dev": [
            "pytest",
            "pytest-cov",
            "black",
            "isort",
            "flake8",
            "mypy",
        ],
    },
    entry_points={
        "console_scripts": [
            "sparvi=sparvi.cli.main:cli",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)