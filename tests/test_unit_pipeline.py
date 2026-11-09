import unittest
from datetime import datetime
from src.ingestion.extract import build_dynamic_query, resolve_table_name
from src.schema.contract import (
    SchemaValidationError,
    parse_columns_config,
    resolve_and_validate_columns,
)
from src.schema.evolution import map_source_type_to_postgres


class TestSchemaContractAndEvolution(unittest.TestCase):
    def test_resolve_table_name_case_insensitive(self):
        db_tables = {"Olist_Orders_Dataset", "olist_customers_dataset"}
        actual = resolve_table_name(db_tables, ["orders", "olist_orders_dataset"])
        self.assertEqual(actual, "Olist_Orders_Dataset")

    def test_missing_primary_key_raises_schema_validation_error(self):
        rule = {
            "strategy": "full",
            "primary_keys": ["order_id"],
            "columns": {"mode": "all"},
        }
        source_columns = [{"name": "customer_id"}, {"name": "order_status"}]
        with self.assertRaises(SchemaValidationError):
            resolve_and_validate_columns("orders", "olist_orders_dataset", rule, source_columns)

    def test_exclude_primary_key_is_forbidden(self):
        rule = {
            "strategy": "full",
            "primary_keys": ["order_id"],
            "columns": {"mode": "exclude", "list": ["order_id"]},
        }
        source_columns = [{"name": "order_id"}, {"name": "order_status"}]
        with self.assertRaises(SchemaValidationError):
            resolve_and_validate_columns("orders", "olist_orders_dataset", rule, source_columns)

    def test_selected_mode_auto_includes_primary_key(self):
        rule = {
            "strategy": "full",
            "primary_keys": ["order_id"],
            "columns": {"mode": "selected", "list": ["order_status"]},
        }
        source_columns = [{"name": "order_id"}, {"name": "order_status"}, {"name": "customer_id"}]
        resolved_cols, mode = resolve_and_validate_columns(
            "orders", "olist_orders_dataset", rule, source_columns
        )
        self.assertEqual(mode, "selected")
        self.assertIn("order_id", resolved_cols)
        self.assertIn("order_status", resolved_cols)
        self.assertNotIn("customer_id", resolved_cols)

    def test_map_source_type_to_postgres(self):
        self.assertEqual(map_source_type_to_postgres("id", {"type": "BIGINT"}), "BIGINT")
        self.assertEqual(map_source_type_to_postgres("qty", {"type": "INT"}), "INTEGER")
        self.assertEqual(map_source_type_to_postgres("price", {"type": "DECIMAL(10, 2)"}), "NUMERIC")
        self.assertEqual(map_source_type_to_postgres("ts", {"type": "DATETIME2"}), "TIMESTAMP")
        self.assertEqual(map_source_type_to_postgres("active", {"type": "BIT"}), "BOOLEAN")

    def test_build_dynamic_query_watermark_and_date_range(self):
        wm_rule = {
            "strategy": "watermark",
            "watermark_column": "order_purchase_timestamp",
            "default_start_watermark": "2017-01-01 00:00:00",
        }
        q_wm = build_dynamic_query(
            logical_name="orders",
            sql_table_name="olist_orders_dataset",
            rule=wm_rule,
            last_watermark="2018-01-01 00:00:00",
            resolved_columns=["order_id", "order_purchase_timestamp"],
            batch_time=datetime(2018, 6, 1, 12, 0, 0),
        )
        self.assertIn("[order_purchase_timestamp] > '2018-01-01 00:00:00'", q_wm)
        self.assertIn("AND [order_purchase_timestamp] <= '2018-06-01 12:00:00'", q_wm)
        self.assertIn("ORDER BY [order_purchase_timestamp] ASC", q_wm)

        dr_rule = {
            "strategy": "date_range",
            "watermark_column": "order_purchase_timestamp",
            "start_date": "2017-01-01",
            "end_date": "2018-01-01",
        }
        q_dr = build_dynamic_query(
            logical_name="orders",
            sql_table_name="olist_orders_dataset",
            rule=dr_rule,
            resolved_columns=["order_id", "order_purchase_timestamp"],
        )
        self.assertIn("[order_purchase_timestamp] >= '2017-01-01' AND [order_purchase_timestamp] < '2018-01-01'", q_dr)

    def test_checkpoint_schema_and_table_definitions(self):
        from src.state import CHECKPOINT_SCHEMA, CHECKPOINT_TABLE, CHUNK_LOG_TABLE
        self.assertEqual(CHECKPOINT_SCHEMA, "etl_metadata")
        self.assertEqual(CHECKPOINT_TABLE, "_pipeline_checkpoints")
        self.assertEqual(CHUNK_LOG_TABLE, "_pipeline_chunk_checkpoints")


if __name__ == "__main__":
    unittest.main()

