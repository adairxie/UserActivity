#-*- encoding: UTF-8 -*-
from setuptools import setup


setup(
    name = "UserActivity",
    version = "1.0",
    include_package_data = True,
    zip_safe = True,

    install_requires = [
        "config",
        "redis",
        "findspark",
        "elasticsearch",
        "python-crontab",
    ],

)
