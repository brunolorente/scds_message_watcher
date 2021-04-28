from setuptools import find_packages, setup
setup(
    name='scds_message_watcher',
    packages=find_packages(include=['scds_message_watcher']),
    version='0.1.0',
    description='Watcher class to persist SCDS messages',
    author='Bruno Lorente',
    license='MIT',
    install_requires=['psycopg2', 'watchdog', 'xmltodict', 'json'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==4.4.1'],
    test_suite='tests',
)