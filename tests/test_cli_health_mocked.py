import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from cfte.cli.main import command_health, ShellContext, PersonalProfile

@pytest.fixture
def mock_context():
    profile = PersonalProfile(
        name="test-profile",
        locale="vi-VN",
        trader={"display_name": "Test Trader", "timezone": "UTC"},
        defaults={},
        scan={},
        live={},
        review={},
        outcomes={},
        ux={}
    )
    return ShellContext(profile_path=Path("configs/profiles/test.yaml"), profile=profile)

def test_command_health_success(mock_context):
    """Verify health command handles successful network audit."""
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        with patch("cfte.storage.sqlite_writer.ThesisSQLiteStore") as mock_store:
            # Mock async DB diagnostics
            store_inst = mock_store.return_value
            store_inst.get_db_diagnostics = MagicMock() # Will be called via asyncio.run
            
            with patch("cfte.cli.reliability.build_runtime_report") as mock_build_report:
                report = MagicMock()
                report.overall_status = "healthy"
                mock_build_report.return_value = report
                
                with patch("cfte.cli.reliability.persist_runtime_report"):
                    with patch("pathlib.Path.exists", return_value=True):
                        # We need to mock asyncio.run because get_db_diagnostics is async
                        with patch("asyncio.run"):
                            code = command_health(mock_context)
                            assert code == 0
                            mock_get.assert_called_once()

def test_command_health_network_error(mock_context):
    """Verify health command handles network failure without crashing."""
    with patch("requests.get", side_effect=Exception("DNS Failure")):
        with patch("cfte.storage.sqlite_writer.ThesisSQLiteStore"):
            with patch("cfte.cli.reliability.build_runtime_report") as mock_build_report:
                report = MagicMock()
                report.overall_status = "healthy"
                mock_build_report.return_value = report
                
                with patch("cfte.cli.reliability.persist_runtime_report"):
                    with patch("pathlib.Path.exists", return_value=False):
                        with patch("asyncio.run"):
                            code = command_health(mock_context)
                            assert code == 0
                            # This verifies that the build_error_surface logic 
                            # we added to main.py doesn't crash the command.

def test_command_health_bad_config(mock_context):
    """Verify health command returns 1 if report status is bad_config."""
    with patch("requests.get"):
        with patch("cfte.storage.sqlite_writer.ThesisSQLiteStore"):
            with patch("cfte.cli.reliability.build_runtime_report") as mock_build_report:
                report = MagicMock()
                report.overall_status = "bad_config"
                mock_build_report.return_value = report
                
                with patch("cfte.cli.reliability.persist_runtime_report"):
                    with patch("pathlib.Path.exists", return_value=False):
                        with patch("asyncio.run"):
                            code = command_health(mock_context)
                            assert code == 1
