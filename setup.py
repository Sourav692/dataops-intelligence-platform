from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]

setup(
    name="dataops-intelligence-platform",
    version="1.0.0",
    author="DataOps Team",
    author_email="dataops@company.com",
    description="Enterprise-grade Databricks job monitoring and intelligence platform",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/company/dataops-intelligence-platform",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0", 
            "black>=23.9.0",
            "flake8>=6.1.0",
            "mypy>=1.6.0"
        ],
        "viz": [
            "matplotlib>=3.7.0",
            "seaborn>=0.12.0", 
            "plotly>=5.15.0"
        ]
    },
    entry_points={
        "console_scripts": [
            "dip=dip.cli.main:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Monitoring",
        "Topic :: Database :: Database Engines/Servers",
    ],
)
