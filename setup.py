import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tap-google-search-console",
    version="0.0.1",
    author="Jules Huisman",
    author_email="jules.huisman@quantile.nl",
    description="A Singer.io tap for Google Search Console",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/quantile-development/tap-google-search-console",
    project_urls={
        "Bug Tracker": "https://github.com/quantile-development/tap-google-search-console/issues",
    },
    install_requires=[
        'singer-sdk==0.3.14',
        'google-api-python-client==2.14.0',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    packages=['tap_google_search_console'],
    entry_points="""
    [console_scripts]
    tap=tap_google_search_console.tap:TapGoogleSearchConsole.cli
    tap-google-search-console=tap_google_search_console.tap:TapGoogleSearchConsole.cli
    """,
    python_requires=">=3.8",
)
