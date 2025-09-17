import os
from app_config.test.common_config_model import TestConfigModel


def test_constructor():
    common_config_model: TestConfigModel = TestConfigModel.load(
        config_file_path="moat/src/app_config/test/config.test.yaml"
    )
    assert common_config_model.db_connection_string == "sqlite:///:memory:"
    assert common_config_model.super_secret == "https:/domain?username=dont-tell-anyone"
    assert common_config_model.overridden == "base"
    assert common_config_model.new_in_layer_2 is None


def test_constructor_with_override():
    common_config_model: TestConfigModel = TestConfigModel.load(
        config_file_path="moat/src/app_config/test/config.test.override.yaml"
    )
    assert common_config_model.db_connection_string == "sqlite:///:memory:"
    assert common_config_model.super_secret == "https:/domain?username=dont-tell-anyone"
    assert common_config_model.overridden == "override"
    assert common_config_model.new_in_layer_2 is None


def test_constructor_with_override_twice():
    common_config_model: TestConfigModel = TestConfigModel.load(
        config_file_path="moat/src/app_config/test/config.test.override_2.yaml"
    )
    assert common_config_model.db_connection_string == "sqlite:///:memory:"
    assert common_config_model.super_secret == "https:/domain?username=dont-tell-anyone"
    assert common_config_model.overridden == "overridden again"
    assert common_config_model.new_in_layer_2 == "new"


def test_get_value():
    # valid item
    assert TestConfigModel.get_value("logger.root_level", "DEBUG") == "INFO"

    # missing item
    assert TestConfigModel.get_value("logger.thing_that_isnt_there", "DEBUG") == "DEBUG"


def test_boolean_value():
    common_config_model: TestConfigModel = TestConfigModel()
    assert common_config_model.boolean_value == False

    common_config_model: TestConfigModel = TestConfigModel.load(
        config_file_path="moat/src/app_config/test/config.test.yaml"
    )
    assert common_config_model.boolean_value == True
