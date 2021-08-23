DEFAULT_HIVE_HOST = "localhost"
DEFAULT_HIVE_PORT = 10000
DEFAULT_HIVE_AUTH_MECHANISM = "PLAIN"


def pytest_addoption(parser):
    """Adds a new command line options to py.test"""
    parser.addoption("--hive_host", default=DEFAULT_HIVE_HOST, help="Hive Host")
    parser.addoption("--hive_port", default=DEFAULT_HIVE_PORT, help="Hive Port")
