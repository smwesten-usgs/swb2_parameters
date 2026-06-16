"""Tests for the v2 pipeline using MN-style data patterns."""
import pandas as pd
import pytest
from pathlib import Path

from swb2_parameters.core import load_long, load_groups, materialize, build_wide, _normalize_code


@pytest.fixture
def tmp_tsv(tmp_path):
    """Helper to write a TSV and return its path."""
    def _write(name: str, content: str) -> Path:
        p = tmp_path / name
        p.write_text(content, encoding="utf-8")
        return p
    return _write


class TestNormalizeCode:
    def test_integer_string(self):
        assert _normalize_code("24") == "24"

    def test_float_string(self):
        assert _normalize_code("24.0") == "24"

    def test_whitespace(self):
        assert _normalize_code("  24  ") == "24"

    def test_empty(self):
        assert _normalize_code("") == ""

    def test_alpha(self):
        assert _normalize_code("24A") == "24A"


class TestLoadLong:
    def test_basic_load(self, tmp_tsv):
        content = "lu_code\tgroup\tparameter\tvalue\tdescription\n1\t\tcn\t71\tCorn\n"
        p = tmp_tsv("test.tsv", content)
        df = load_long([p])
        assert len(df) == 1
        assert df.iloc[0]["parameter"] == "cn"

    def test_rejects_space_delimited(self, tmp_tsv):
        content = "lu_code group parameter value\n1  cn 71\n"
        p = tmp_tsv("bad.tsv", content)
        with pytest.raises(ValueError, match="not tab-delimited"):
            load_long([p])

    def test_multiple_files(self, tmp_tsv):
        c1 = "lu_code\tgroup\tparameter\tvalue\n1\t\tcn\t71\n"
        c2 = "lu_code\tgroup\tparameter\tvalue\n2\t\tcn\t60\n"
        p1 = tmp_tsv("a.tsv", c1)
        p2 = tmp_tsv("b.tsv", c2)
        df = load_long([p1, p2])
        assert len(df) == 2


class TestMaterialize:
    def test_all_template(self, tmp_tsv):
        content = (
            "lu_code\tgroup\tparameter\tvalue\tdescription\n"
            "\tALL\trew_1\t0.12\t\n"
            "1\t\tcn\t71\tCorn\n"
            "2\t\tcn\t60\tSoybeans\n"
        )
        p = tmp_tsv("long.tsv", content)
        df = load_long([p])
        result = materialize(df)
        # Both codes should have rew_1
        rew_rows = result[result["parameter"] == "rew_1"]
        assert set(rew_rows["lu_code"]) == {"1", "2"}

    def test_named_group_overrides_all(self, tmp_tsv):
        content = (
            "lu_code\tgroup\tparameter\tvalue\tdescription\n"
            "\tALL\tcn\t71\t\n"
            "\tsmgrain\tcn\t60\t\n"
            "1\t\trz\t1.4\tCorn\n"
            "2\t\trz\t3.9\tSoybeans\n"
        )
        p = tmp_tsv("long.tsv", content)
        df = load_long([p])
        groups = {"1": "smgrain", "2": "other"}
        result = materialize(df, groups)
        # Code 1 is in smgrain → cn=60; code 2 gets ALL → cn=71
        cn_1 = result[(result["lu_code"] == "1") & (result["parameter"] == "cn")]
        cn_2 = result[(result["lu_code"] == "2") & (result["parameter"] == "cn")]
        assert cn_1.iloc[0]["value"] == "60"
        assert cn_2.iloc[0]["value"] == "71"

    def test_code_specific_wins(self, tmp_tsv):
        content = (
            "lu_code\tgroup\tparameter\tvalue\tdescription\n"
            "\tALL\tcn\t71\t\n"
            "1\t\tcn\t55\tCorn\n"
        )
        p = tmp_tsv("long.tsv", content)
        df = load_long([p])
        result = materialize(df)
        cn_1 = result[(result["lu_code"] == "1") & (result["parameter"] == "cn")]
        assert cn_1.iloc[0]["value"] == "55"


class TestBuildWide:
    def test_family_expansion(self, tmp_tsv):
        content = (
            "lu_code\tgroup\tparameter\tvalue\tdescription\n"
            "1\t\tcn\t71\tCorn\n"
        )
        p = tmp_tsv("long.tsv", content)
        df = load_long([p])
        df = materialize(df)
        wide = build_wide(df, families=["cn"])
        assert "cn_1" in wide.columns
        assert "cn_7" in wide.columns
        assert wide.iloc[0]["cn_1"] == 71
        # cn_2 should be round(37.8 + 0.622*71) = 82
        assert wide.iloc[0]["cn_2"] == 82

    def test_singleton_passthrough(self, tmp_tsv):
        content = (
            "lu_code\tgroup\tparameter\tvalue\tdescription\n"
            "1\t\trew_1\t0.12\tCorn\n"
            "1\t\tapplication_scheme\tfield_capacity\tCorn\n"
        )
        p = tmp_tsv("long.tsv", content)
        df = load_long([p])
        df = materialize(df)
        wide = build_wide(df, families=[])
        assert wide.iloc[0]["rew_1"] == 0.12
        assert wide.iloc[0]["application_scheme"] == "field_capacity"

    def test_no_expansion_when_family_disabled(self, tmp_tsv):
        content = (
            "lu_code\tgroup\tparameter\tvalue\tdescription\n"
            "1\t\tcn\t71\tCorn\n"
        )
        p = tmp_tsv("long.tsv", content)
        df = load_long([p])
        df = materialize(df)
        wide = build_wide(df, families=[])
        # cn treated as singleton
        assert "cn" in wide.columns
        assert "cn_1" not in wide.columns


class TestEndToEnd:
    """Test that mimics the MN data pattern: ALL templates + code-specific values."""

    def test_mn_pattern(self, tmp_tsv):
        # Simulate: REW/TEW as ALL, CN/RZ as code-specific singletons (no expansion)
        content = (
            "lu_code\tgroup\tparameter\tvalue\tdescription\n"
            # ALL templates (constant across codes)
            "\tALL\trew_1\t0.12\t\n"
            "\tALL\trew_2\t0.23\t\n"
            "\tALL\ttew_1\t0.28\t\n"
            "\tALL\ttew_2\t0.60\t\n"
            "\tALL\tl_fallow\t1\t\n"
            # Code-specific (per-code hand-tuned HSG values as singletons)
            "1\t\tcn_1\t71.0\tCorn\n"
            "1\t\tcn_2\t82.0\tCorn\n"
            "1\t\tcn_3\t88.1\tCorn\n"
            "1\t\trz_1\t1.40\tCorn\n"
            "1\t\trz_2\t1.46\tCorn\n"
            "2\t\tcn_1\t71.0\tCotton\n"
            "2\t\tcn_2\t80.0\tCotton\n"
            "2\t\tcn_3\t87.0\tCotton\n"
            "2\t\trz_1\t1.50\tCotton\n"
            "2\t\trz_2\t1.42\tCotton\n"
        )
        p = tmp_tsv("mn_style.tsv", content)
        df = load_long([p])
        result = materialize(df)
        wide = build_wide(result, families=[])  # No expansion — values are direct

        assert len(wide) == 2
        row1 = wide[wide["lu_code"] == "1"].iloc[0]
        assert row1["cn_1"] == 71.0
        assert row1["cn_2"] == 82.0
        assert row1["rew_1"] == 0.12
        assert row1["tew_2"] == 0.60
        assert row1["l_fallow"] == 1

        row2 = wide[wide["lu_code"] == "2"].iloc[0]
        assert row2["cn_2"] == 80.0
        assert row2["rz_2"] == 1.42
        assert row2["rew_1"] == 0.12  # from ALL template
