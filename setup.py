from setuptools import find_packages, setup


setup(
    name="poker-schedule-controller",
    version="0.1.0",
    packages=find_packages(include=("schedule_controller", "schedule_controller.*")),
)
