import pytest
import requests
from mcp.server.mcpserver.exceptions import ToolError

from entrypoints.mcp_server import _expected_errors


def test_expected_errors_surfaces_request_failures():
    @_expected_errors
    def tool():
        raise requests.exceptions.ConnectionError("connection reset")

    with pytest.raises(ToolError, match="ConnectionError.*connection reset"):
        tool()


def test_expected_errors_logs_and_reraises_unexpected(caplog):
    @_expected_errors
    def tool():
        raise KeyError("Dados")

    with pytest.raises(KeyError):
        tool()
    assert "Unexpected error in tool tool" in caplog.text
