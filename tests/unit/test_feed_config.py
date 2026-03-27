"""Tests for feed config models and loader."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from api.models import (
    FeedConfig,
    FeedFieldDefinition,
    FeedSourceConfig,
    FeedStatus,
    SilverProcessingType,
    SourceFormat,
)
from pipeline.feed_config import compute_schema_hash, load_feed_configs, normalize_column_name


@pytest.fixture()
def valid_feed_dict() -> dict:
    return {
        "feed_id": "test_feed",
        "name": "Test Feed",
        "version": "1.0.0",
        "status": "active",
        "source": {
            "type": "api",
            "url": "https://example.com/api",
            "format": "json",
            "auth_method": "none",
            "rate_limit_rpm": 30,
        },
        "schema_fields": [
            {
                "name": "data",
                "source_field": "data",
                "type": "string",
                "required": True,
                "description": "Date field",
                "silver_type": "DATE",
                "silver_expression": 'strptime("{col}", \'%d/%m/%Y\')',
            },
            {
                "name": "valor",
                "source_field": "valor",
                "type": "string",
                "required": True,
                "description": "Value field",
                "silver_type": "DOUBLE",
                "silver_expression": 'CAST("{col}" AS DOUBLE)',
            },
        ],
        "metadata": {
            "unit": "%",
            "tags": ["test"],
            "domain": "test",
        },
    }


class TestFeedConfigModel:
    def test_validates_valid_dict(self, valid_feed_dict: dict) -> None:
        feed = FeedConfig.model_validate(valid_feed_dict, strict=False)
        assert feed.feed_id == "test_feed"
        assert feed.name == "Test Feed"
        assert feed.status == FeedStatus.ACTIVE
        assert feed.source.format == SourceFormat.JSON
        assert len(feed.schema_fields) == 2
        assert feed.schema_fields[0].silver_type == "DATE"

    def test_rejects_extra_fields(self, valid_feed_dict: dict) -> None:
        valid_feed_dict["unknown_field"] = "should fail"
        with pytest.raises(Exception):
            FeedConfig.model_validate(valid_feed_dict, strict=False)

    def test_rejects_missing_required_fields(self) -> None:
        with pytest.raises(Exception):
            FeedConfig.model_validate({"name": "Missing feed_id"})

    def test_defaults_applied(self, valid_feed_dict: dict) -> None:
        feed = FeedConfig.model_validate(valid_feed_dict, strict=False)
        assert feed.schedule.cron == "0 6 * * *"
        assert feed.schedule.timezone == "UTC"
        assert feed.processing.bronze.write_mode == "append"
        assert feed.processing.bronze.rescued_data_column == "_rescued_data"
        assert feed.processing.silver.processing_type == SilverProcessingType.LATEST_ONLY

    def test_quality_defaults(self, valid_feed_dict: dict) -> None:
        feed = FeedConfig.model_validate(valid_feed_dict, strict=False)
        assert feed.quality.bronze.max_null_rate == 0.02
        assert feed.quality.bronze.min_row_count == 1
        assert feed.quality.silver is None
        assert feed.quality.gold is None

    def test_csv_source_format(self) -> None:
        source = FeedSourceConfig(
            url="https://example.com/data.csv",
            format=SourceFormat.CSV,
            csv_separator=";",
        )
        assert source.format == SourceFormat.CSV
        assert source.csv_separator == ";"

    def test_silver_processing_types(self) -> None:
        assert SilverProcessingType.APPEND.value == "append"
        assert SilverProcessingType.MERGE_BY_KEY.value == "merge_by_key"
        assert SilverProcessingType.LATEST_ONLY.value == "latest_only"

    def test_field_definition_optional_silver(self) -> None:
        field = FeedFieldDefinition(
            name="test",
            source_field="test",
            type="string",
        )
        assert field.silver_type is None
        assert field.silver_expression is None
        assert field.required is False


class TestNormalizeColumnName:
    def test_lowercase(self) -> None:
        assert normalize_column_name("NC") == "nc"

    def test_spaces_to_underscores(self) -> None:
        assert normalize_column_name("Data Base") == "data_base"

    def test_multiple_spaces(self) -> None:
        assert normalize_column_name("Taxa Compra Manha") == "taxa_compra_manha"

    def test_special_chars(self) -> None:
        assert normalize_column_name("PU (Compra)") == "pu_compra"

    def test_accents_removed(self) -> None:
        assert normalize_column_name("Variacao") == "variacao"

    def test_already_clean(self) -> None:
        assert normalize_column_name("data") == "data"
        assert normalize_column_name("valor") == "valor"

    def test_consecutive_underscores_collapsed(self) -> None:
        assert normalize_column_name("a  b--c") == "a_b_c"

    def test_leading_trailing_stripped(self) -> None:
        assert normalize_column_name(" name ") == "name"


class TestSchemaHash:
    def test_deterministic(self, valid_feed_dict: dict) -> None:
        feed = FeedConfig.model_validate(valid_feed_dict, strict=False)
        hash1 = compute_schema_hash(feed)
        hash2 = compute_schema_hash(feed)
        assert hash1 == hash2
        assert len(hash1) == 16

    def test_changes_with_different_fields(self, valid_feed_dict: dict) -> None:
        feed1 = FeedConfig.model_validate(valid_feed_dict, strict=False)
        valid_feed_dict["schema_fields"].append({
            "name": "extra_field",
            "source_field": "extra",
            "type": "string",
        })
        feed2 = FeedConfig.model_validate(valid_feed_dict, strict=False)
        assert compute_schema_hash(feed1) != compute_schema_hash(feed2)

    def test_order_independent(self, valid_feed_dict: dict) -> None:
        feed1 = FeedConfig.model_validate(valid_feed_dict, strict=False)
        # Reverse the schema fields
        valid_feed_dict["schema_fields"] = list(reversed(valid_feed_dict["schema_fields"]))
        feed2 = FeedConfig.model_validate(valid_feed_dict, strict=False)
        assert compute_schema_hash(feed1) == compute_schema_hash(feed2)


class TestFeedConfigLoader:
    def test_loads_all_yamls(self, tmp_path: Path) -> None:
        feed1 = {
            "feed_id": "feed_a",
            "name": "Feed A",
            "status": "active",
            "source": {"url": "https://a.com", "format": "json"},
        }
        feed2 = {
            "feed_id": "feed_b",
            "name": "Feed B",
            "status": "active",
            "source": {"url": "https://b.com", "format": "json"},
        }
        (tmp_path / "a.yaml").write_text(yaml.dump(feed1))
        (tmp_path / "b.yaml").write_text(yaml.dump(feed2))

        configs = load_feed_configs(tmp_path)
        assert len(configs) == 2
        assert "feed_a" in configs
        assert "feed_b" in configs

    def test_filters_inactive(self, tmp_path: Path) -> None:
        active = {
            "feed_id": "active_feed",
            "name": "Active",
            "status": "active",
            "source": {"url": "https://a.com", "format": "json"},
        }
        paused = {
            "feed_id": "paused_feed",
            "name": "Paused",
            "status": "paused",
            "source": {"url": "https://b.com", "format": "json"},
        }
        (tmp_path / "active.yaml").write_text(yaml.dump(active))
        (tmp_path / "paused.yaml").write_text(yaml.dump(paused))

        configs = load_feed_configs(tmp_path)
        assert len(configs) == 1
        assert "active_feed" in configs

    def test_include_inactive(self, tmp_path: Path) -> None:
        paused = {
            "feed_id": "paused_feed",
            "name": "Paused",
            "status": "paused",
            "source": {"url": "https://b.com", "format": "json"},
        }
        (tmp_path / "paused.yaml").write_text(yaml.dump(paused))

        configs = load_feed_configs(tmp_path, include_inactive=True)
        assert len(configs) == 1

    def test_empty_dir(self, tmp_path: Path) -> None:
        configs = load_feed_configs(tmp_path)
        assert configs == {}

    def test_missing_dir(self) -> None:
        configs = load_feed_configs("/nonexistent/path")
        assert configs == {}

    def test_skips_invalid_yaml(self, tmp_path: Path) -> None:
        valid = {
            "feed_id": "good",
            "name": "Good",
            "status": "active",
            "source": {"url": "https://a.com", "format": "json"},
        }
        (tmp_path / "good.yaml").write_text(yaml.dump(valid))
        (tmp_path / "bad.yaml").write_text("feed_id: missing_name\n")

        configs = load_feed_configs(tmp_path)
        assert len(configs) == 1
        assert "good" in configs

    def test_loads_production_feeds(self) -> None:
        """Verify the actual config/feeds/br_macro/ YAML files load correctly."""
        configs = load_feed_configs("config/feeds/br_macro")
        assert len(configs) >= 9
        assert "bcb_selic" in configs
        assert "bcb_ipca" in configs
        assert "bcb_usd_brl" in configs
        assert "ibge_pnad" in configs
        assert "ibge_gdp" in configs
        assert "tesouro" in configs
        assert "tesouro_prefixado_curto" in configs
        assert "tesouro_prefixado_longo" in configs
        assert "tesouro_ipca" in configs

        # Verify IBGE PNAD has all 11 fields
        ibge = configs["ibge_pnad"]
        assert len(ibge.schema_fields) == 11

        # Verify Tesouro is CSV format
        tesouro = configs["tesouro"]
        assert tesouro.source.format == SourceFormat.CSV
        assert tesouro.source.csv_separator == ";"

        # Verify derived feeds reference tesouro bronze
        for derived_id in ("tesouro_prefixado_curto", "tesouro_prefixado_longo", "tesouro_ipca"):
            derived = configs[derived_id]
            assert derived.bronze_source == "tesouro"
            assert derived.processing.silver.aggregation == "avg"

    def test_bcb_backfill_fields(self) -> None:
        """BCB feeds should have backfill configuration."""
        configs = load_feed_configs("config/feeds/br_macro")
        for feed_id in ("bcb_selic", "bcb_ipca", "bcb_usd_brl"):
            feed = configs[feed_id]
            assert feed.source.backfill_url is not None
            assert feed.source.backfill_window_years == 10
            assert feed.source.backfill_start_date is not None
            assert "{start}" in feed.source.backfill_url
            assert "{end}" in feed.source.backfill_url

    def test_ibge_pnad_backfill_fields(self) -> None:
        """IBGE PNAD should have backfill URL with p/all."""
        configs = load_feed_configs("config/feeds/br_macro")
        feed = configs["ibge_pnad"]
        assert feed.source.backfill_url is not None
        assert "p/all" in feed.source.backfill_url
        assert feed.source.backfill_window_years is None

    def test_gdp_backfill_fields(self) -> None:
        """GDP feed should have BCB-style backfill with windowing."""
        configs = load_feed_configs("config/feeds/br_macro")
        feed = configs["ibge_gdp"]
        assert feed.source.backfill_url is not None
        assert feed.source.backfill_window_years == 10
        assert "{start}" in feed.source.backfill_url
