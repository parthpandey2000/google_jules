import unittest
import yaml
import os
import sys

sys.path.insert(0, '.') # Add project root to path

from src.ingestion.reader import load_source_config
from src.transformations.transformer import load_transformation_config
from src.loading.writer import load_target_config
from src.data_quality.validator import load_dq_rules

class TestConfigLoaders(unittest.TestCase):

    def setUp(self):
        # Create dummy config files for testing
        self.test_dir = "temp_test_configs"
        os.makedirs(self.test_dir, exist_ok=True)

        self.sources_data = {
            "sources": [
                {"name": "cat.schema.src_table1", "alias": "st1"},
                {"name": "cat.schema.src_table2", "alias": "st2"}
            ]
        }
        self.sources_file_path = os.path.join(self.test_dir, "sources.yaml")
        with open(self.sources_file_path, 'w') as f:
            yaml.dump(self.sources_data, f)

        self.targets_data = {
            "targets": [
                {"name": "cat.schema.target1", "alias": "t1", "path": "/path/t1"},
                {"name": "cat.schema.target2", "alias": "t2", "path": "/path/t2"}
            ]
        }
        self.targets_file_path = os.path.join(self.test_dir, "targets.yaml")
        with open(self.targets_file_path, 'w') as f:
            yaml.dump(self.targets_data, f)

        self.transformations_data = {
            "transformations": [
                {"target_table": "t1", "sources": ["st1"]},
                {"target_table": "t2", "sources": ["st1", "st2"]}
            ]
        }
        self.transformations_file_path = os.path.join(self.test_dir, "transformations.yaml")
        with open(self.transformations_file_path, 'w') as f:
            yaml.dump(self.transformations_data, f)

        self.dq_rules_data = {
            "st1": {"rules": [{"column": "colA", "type": "not_null"}]},
            "t1": {"rules": [{"column": "colB", "type": "unique"}]}
        }
        self.dq_rules_file_path = os.path.join(self.test_dir, "dq_rules.yaml")
        with open(self.dq_rules_file_path, 'w') as f:
            yaml.dump(self.dq_rules_data, f)

        self.empty_config_path = os.path.join(self.test_dir, "empty.yaml")
        with open(self.empty_config_path, 'w') as f:
            yaml.dump({}, f) # Empty YAML content

        self.malformed_config_path = os.path.join(self.test_dir, "malformed.yaml")
        with open(self.malformed_config_path, 'w') as f:
            f.write("sources: [\n  {'name': 'table_missing_quote\n") # Malformed YAML


    def tearDown(self):
        # Clean up dummy config files and directory
        if os.path.exists(self.sources_file_path): os.remove(self.sources_file_path)
        if os.path.exists(self.targets_file_path): os.remove(self.targets_file_path)
        if os.path.exists(self.transformations_file_path): os.remove(self.transformations_file_path)
        if os.path.exists(self.dq_rules_file_path): os.remove(self.dq_rules_file_path)
        if os.path.exists(self.empty_config_path): os.remove(self.empty_config_path)
        if os.path.exists(self.malformed_config_path): os.remove(self.malformed_config_path)
        if os.path.exists(self.test_dir): os.rmdir(self.test_dir)

    def test_load_source_config_success(self):
        config = load_source_config(self.sources_file_path)
        self.assertEqual(config, self.sources_data["sources"])

    def test_load_target_config_success(self):
        config = load_target_config(self.targets_file_path)
        self.assertEqual(config, self.targets_data["targets"])

    def test_load_transformation_config_success(self):
        config = load_transformation_config(self.transformations_file_path)
        self.assertEqual(config, self.transformations_data["transformations"])

    def test_load_dq_rules_config_success(self):
        config = load_dq_rules(self.dq_rules_file_path)
        self.assertEqual(config, self.dq_rules_data)

    def test_load_config_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            load_source_config("non_existent_file.yaml")
        with self.assertRaises(FileNotFoundError):
            load_target_config("non_existent_file.yaml")
        with self.assertRaises(FileNotFoundError):
            load_transformation_config("non_existent_file.yaml")
        with self.assertRaises(FileNotFoundError):
            load_dq_rules("non_existent_file.yaml")

    def test_load_empty_source_config(self):
        # Test if the main key (e.g., "sources") is missing in the YAML
        empty_data_file = os.path.join(self.test_dir, "empty_data.yaml")
        with open(empty_data_file, 'w') as f:
            yaml.dump({"some_other_key": []}, f)

        config = load_source_config(empty_data_file)
        self.assertEqual(config, []) # Expect default empty list
        os.remove(empty_data_file)

    def test_load_empty_targets_config(self):
        empty_data_file = os.path.join(self.test_dir, "empty_data.yaml")
        with open(empty_data_file, 'w') as f:
            yaml.dump({"some_other_key": []}, f)
        config = load_target_config(empty_data_file)
        self.assertEqual(config, [])
        os.remove(empty_data_file)

    def test_load_empty_transformations_config(self):
        empty_data_file = os.path.join(self.test_dir, "empty_data.yaml")
        with open(empty_data_file, 'w') as f:
            yaml.dump({"some_other_key": []}, f)
        config = load_transformation_config(empty_data_file)
        self.assertEqual(config, [])
        os.remove(empty_data_file)

    def test_load_empty_dq_rules_config(self):
        # load_dq_rules returns the whole dict, so an empty file means empty dict
        config = load_dq_rules(self.empty_config_path)
        self.assertEqual(config, {})


    def test_load_malformed_yaml(self):
        with self.assertRaises(yaml.YAMLError):
            load_source_config(self.malformed_config_path)
        with self.assertRaises(yaml.YAMLError):
            load_target_config(self.malformed_config_path)
        with self.assertRaises(yaml.YAMLError):
            load_transformation_config(self.malformed_config_path)
        with self.assertRaises(yaml.YAMLError):
            load_dq_rules(self.malformed_config_path)


if __name__ == "__main__":
    # To run these tests from the command line from the root directory:
    # python -m unittest tests.test_config_loaders
    unittest.main()
