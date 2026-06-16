"""Tests for the consolidated swb2_parameters pipeline."""
import pandas as pd
import pytest
from pathlib import Path

from swb2_parameters.config import load_config, Config, TableSpec
from swb2_parameters.io import load_long
from swb2_parameters.groups import materialize_groups, _normalize_code
from swb2_parameters.validate import validate
from swb2_parameters.build import build_tables


@pytest.fixture
def tmp_tsv(tmp_path):
    """Helper to write a TSV and return its path."""
    def _write(name: str, content: str) -> Path:
        p = tmp_path / name
        p.write_text(content, encoding="utf-8")
        return p
    return _write


@pytest.fixture
def basic_config():
    """Minimal config for testing."""
    return Config(
        hsg_count=7,
        groups={"smgrain": [21, 22, 23, 24, 25]},
        tables={"lu_lookup": TableSpec(
            families=["cn", "rz"],
            singletons=["growing_season_interception_a"],
        )},
    )


# --- Config ---

class TestConfig:
    def test_load_example_toml(self):
        cfg = load_config(Path(__file__).parent.parent / "example" / "swb2_parameters.toml")
        assert cfg.hsg_count == 7
        assert "smgrain" in cfg.groups
        assert 24 in cfg.groups["smgrain"]
        assert "lu_lookup" in cfg.tables
        assert "irr_lookup" in cfg.tables
        assert "cn" in cfg.tables["lu_lookup"].families

    def test_missing_tables_raises(self, tmp_tsv):
        p = tmp_tsv("bad.toml", '[long_schema]\nhsg_count = 7\n')
        with pytest.raises(ValueError, match="at least one"):
            load_config(p)


# --- I/O ---

class TestIO:
    def test_load_basic(self, tmp_tsv):
        content = "lu_code\tdescription\tgroup\tcolumn\tparlbnd\tparubnd\tparval1\n1\tCorn\t\tcn\t63\t78\t71\n"
        p = tmp_tsv("test.tsv", content)
        df = load_long([p])
        assert len(df) == 1
        assert df.iloc[0]["column"] == "cn"
        assert df.iloc[0]["parlbnd"] == 63.0

    def test_rejects_space_delimited(self, tmp_tsv):
        content = "lu_code description group column\n1 Corn  cn\n"
        p = tmp_tsv("bad.tsv", content)
        with pytest.raises(ValueError, match="not tab-delimited"):
            load_long([p])

    def test_normalizes_family_name(self, tmp_tsv):
        content = "lu_code\tcolumn\tparval1\n1\tmax_net_infiltration\t3.0\n"
        p = tmp_tsv("alias.tsv", content)
        df = load_long([p])
        assert df.iloc[0]["column"] == "max_net_infil"

    def test_normalizes_drained_condition(self, tmp_tsv):
        content = "lu_code\tcolumn\tparval1\tdrained_condition\n1\tcn\t71\t\n"
        p = tmp_tsv("dc.tsv", content)
        df = load_long([p])
        assert df.iloc[0]["drained_condition"] == "drained"


# --- Groups ---

class TestGroups:
    def test_normalize_code(self):
        assert _normalize_code("24.0") == "24"
        assert _normalize_code("  24  ") == "24"
        assert _normalize_code("") == ""
        assert _normalize_code("24A") == "24A"

    def test_all_template(self, tmp_tsv, basic_config):
        content = (
            "lu_code\tdescription\tgroup\tcolumn\tparlbnd\tparubnd\tparval1\n"
            "\t\tALL\trz\t1.0\t3.0\t2.0\n"
            "1\tCorn\t\tcn\t63\t78\t71\n"
            "2\tSoy\t\tcn\t55\t70\t60\n"
        )
        p = tmp_tsv("long.tsv", content)
        df = load_long([p])
        result = materialize_groups(df, basic_config)
        rz_rows = result[result["column"] == "rz"]
        assert set(rz_rows["lu_code"]) == {"1", "2"}

    def test_code_specific_wins(self, tmp_tsv, basic_config):
        content = (
            "lu_code\tdescription\tgroup\tcolumn\tparlbnd\tparubnd\tparval1\n"
            "\t\tALL\tcn\t50\t80\t65\n"
            "1\tCorn\t\tcn\t63\t78\t71\n"
        )
        p = tmp_tsv("long.tsv", content)
        df = load_long([p])
        result = materialize_groups(df, basic_config)
        cn_1 = result[(result["lu_code"] == "1") & (result["column"] == "cn")]
        assert float(cn_1.iloc[0]["parval1"]) == 71.0

    def test_named_group_overrides_all(self, tmp_tsv, basic_config):
        content = (
            "lu_code\tdescription\tgroup\tcolumn\tparlbnd\tparubnd\tparval1\n"
            "\t\tALL\tcn\t50\t90\t70\n"
            "\t\tsmgrain\tcn\t40\t80\t55\n"
            "21\tWheat\t\trz\t1.0\t3.0\t2.0\n"
            "99\tOther\t\trz\t1.0\t3.0\t2.0\n"
        )
        p = tmp_tsv("long.tsv", content)
        df = load_long([p])
        result = materialize_groups(df, basic_config)
        cn_21 = result[(result["lu_code"] == "21") & (result["column"] == "cn")]
        assert float(cn_21.iloc[0]["parval1"]) == 55.0
        cn_99 = result[(result["lu_code"] == "99") & (result["column"] == "cn")]
        assert float(cn_99.iloc[0]["parval1"]) == 70.0


# --- Validate ---

class TestValidate:
    def test_duplicates_raise(self, tmp_tsv):
        content = (
            "lu_code\tdescription\tgroup\tcolumn\tparlbnd\tparubnd\tparval1\n"
            "1\tCorn\t\tcn\t63\t78\t71\n"
            "1\tCorn\t\tcn\t63\t78\t72\n"
        )
        p = tmp_tsv("dup.tsv", content)
        df = load_long([p])
        with pytest.raises(ValueError, match="Duplicate"):
            validate(df)

    def test_bounded_rounding_cn(self, tmp_tsv):
        content = "lu_code\tdescription\tgroup\tcolumn\tparlbnd\tparubnd\tparval1\n1\tCorn\t\tcn\t63\t78\t71.6\n"
        p = tmp_tsv("round.tsv", content)
        df = load_long([p])
        result = validate(df)
        assert result.iloc[0]["parval1"] == 72  # cn rounds to integer

    def test_fixed_family_raises(self, tmp_tsv):
        content = "lu_code\tdescription\tgroup\tcolumn\tparlbnd\tparubnd\tparval1\tfixed_parval1\n1\tCorn\t\tcn\t63\t78\t71\t99\n"
        p = tmp_tsv("fixed_fam.tsv", content)
        df = load_long([p])
        with pytest.raises(ValueError, match="FIXED"):
            validate(df)

    def test_fixed_singleton_passthrough(self, tmp_tsv):
        content = "lu_code\tdescription\tgroup\tcolumn\tparlbnd\tparubnd\tparval1\tfixed_parval1\n1\tCorn\t\trew_1\t\t\t\t0.15\n"
        p = tmp_tsv("fixed_sing.tsv", content)
        df = load_long([p])
        result = validate(df)
        assert result.iloc[0]["parval1"] == "0.15"


# --- Build ---

class TestBuild:
    def test_cn_expansion(self, tmp_tsv, basic_config):
        content = "lu_code\tdescription\tgroup\tcolumn\tparlbnd\tparubnd\tparval1\n1\tCorn\t\tcn\t63\t78\t71\n"
        p = tmp_tsv("long.tsv", content)
        df = load_long([p])
        df = validate(df)
        tables = build_tables(df, basic_config)
        wide = tables["lu_lookup"]
        assert "cn_1" in wide.columns
        assert "cn_7" in wide.columns
        assert wide.iloc[0]["cn_1"] == 71
        assert wide.iloc[0]["cn_2"] == 82  # round(37.8 + 0.622*71)

    def test_output_has_lu_code(self, tmp_tsv, basic_config):
        content = "lu_code\tdescription\tgroup\tcolumn\tparlbnd\tparubnd\tparval1\n1\tCorn\t\tcn\t63\t78\t71\n"
        p = tmp_tsv("long.tsv", content)
        df = load_long([p])
        df = validate(df)
        tables = build_tables(df, basic_config)
        assert "lu_code" in tables["lu_lookup"].columns
        assert tables["lu_lookup"].iloc[0]["lu_code"] == "1"


# --- End-to-end with example data ---

class TestEndToEnd:
    def test_build_lu_from_example(self):
        """Test build using the actual example long-form data."""
        example_dir = Path(__file__).parent.parent / "example"
        cfg = load_config(example_dir / "swb2_parameters.toml")
        df = load_long([example_dir / "lu_params_long_all_cdl.tsv"])
        df = materialize_groups(df, cfg)
        df = validate(df)
        tables = build_tables(df, cfg)

        lu = tables["lu_lookup"]
        assert "lu_code" in lu.columns
        assert "cn_1" in lu.columns
        assert "cn_7" in lu.columns
        assert "rz_1" in lu.columns
        assert len(lu) > 0
        # First row (code 1 = Corn): cn_1 should be 71
        corn = lu[lu["lu_code"] == "1"].iloc[0]
        assert corn["cn_1"] == 71

    def test_all_template_broadcast(self):
        """Test that ALL templates (rew_and_tew, max_net_infiltration) broadcast."""
        example_dir = Path(__file__).parent.parent / "example"
        cfg = load_config(example_dir / "swb2_parameters.toml")
        df = load_long([
            example_dir / "lu_params_long_all_cdl.tsv",
            example_dir / "max_net_infiltration.tsv",
        ])
        df = materialize_groups(df, cfg)
        df = validate(df)
        tables = build_tables(df, cfg)

        lu = tables["lu_lookup"]
        # max_net_infil_1 should be 2.6 for all codes (from ALL template)
        assert (lu["max_net_infil_1"] == 2.6).all()
