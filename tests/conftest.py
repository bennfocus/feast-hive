DEFAULT_HS2_HOST = "localhost"
DEFAULT_HS2_PORT = 10000
DEFAULT_DATABASE = "default"


def pytest_addoption(parser):
    """Adds a new command line options to py.test"""
    parser.addoption("--host", default=DEFAULT_HS2_HOST, help="HiveServer2 Host")
    parser.addoption("--port", default=DEFAULT_HS2_PORT, help="HiveServer2 Port")
    parser.addoption("--database", default=DEFAULT_DATABASE, help="Default Database")
