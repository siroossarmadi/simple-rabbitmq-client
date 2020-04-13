from setuptools import setup, find_packages

setup(
    name="rabbitmq_client",
    version="0.1",
    packages=find_packages(),
    install_requires="pika==1.1.0"
)
