"""
Test suite for DataOps Intelligence Platform
"""

import pytest
from unittest.mock import Mock, patch
from src.dip.core.config import DatabricksConfig, Config
from src.dip.orchestrator import DataOpsOrchestrator


class TestConfig:
    def test_databricks_config_creation(self):
        """Test DatabricksConfig creation"""
        config = DatabricksConfig(
            workspace_url="https://test.databricks.com",
            access_token="test-token"
        )
        assert config.workspace_url == "https://test.databricks.com"
        assert config.access_token == "test-token"

    def test_config_from_file(self):
        """Test Config loading from file"""
        # This would require a test config file
        pass


class TestOrchestrator:
    @patch('src.dip.orchestrator.DatabricksConnector')
    def test_orchestrator_initialization(self, mock_connector):
        """Test orchestrator initialization"""
        config = Config()
        orchestrator = DataOpsOrchestrator(config)
        assert orchestrator.config == config


if __name__ == "__main__":
    pytest.main([__file__])
