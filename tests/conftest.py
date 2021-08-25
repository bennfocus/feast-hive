DEFAULT_HS2_HOST = "localhost"
DEFAULT_HS2_PORT = 10000


def pytest_addoption(parser):
    """Adds a new command line options to py.test"""
    parser.addoption("--hs2_host", default=DEFAULT_HS2_HOST, help="HiveServer2 Host")
    parser.addoption("--hs2_port", default=DEFAULT_HS2_PORT, help="HiveServer2 Port")
    parser.addoption(
        "--hive_table_prefix",
        help="To use existed table without upload, for reducing test running time.",
    )
