"""
Tests for the Zero-Code ETL Framework.
Run with: pytest tests/ -v
"""

import json
import os
import tempfile

import pytest

from etl_core.extractors import (
    CSVExtractor,
    JSONFileExtractor,
    ExtractorFactory,
)
from etl_core.transformers import (
    RenameColumnsTransformer,
    FilterRowsTransformer,
    CastTypesTransformer,
    AddColumnTransformer,
    DropColumnsTransformer,
    DeduplicateTransformer,
    FillNullsTransformer,
    NormalizeTextTransformer,
    TransformerPipeline,
    TransformerFactory,
)
from etl_core.loaders import CSVLoader, JSONFileLoader, LoaderFactory
from etl_core.engine import ETLEngine


# ------------------------------------------------------------------ #
#  Fixtures                                                             #
# ------------------------------------------------------------------ #

SAMPLE_DATA = [
    {"id": "1", "name": "  Alice ", "email": "ALICE@EXAMPLE.COM", "score": "90"},
    {"id": "2", "name": "Bob", "email": "bob@example.com", "score": "70"},
    {"id": "3", "name": "Alice", "email": "ALICE@EXAMPLE.COM", "score": "85"},  # duplicate email
    {"id": "4", "name": "Carol", "email": "carol@example.com", "score": None},
]


@pytest.fixture
def sample_data():
    return [dict(row) for row in SAMPLE_DATA]


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


# ------------------------------------------------------------------ #
#  Extractor tests                                                      #
# ------------------------------------------------------------------ #

class TestCSVExtractor:
    def test_basic_read(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        with open(path, "w") as f:
            f.write("name,age\nAlice,30\nBob,25\n")
        extractor = CSVExtractor({"path": path})
        data = extractor.extract()
        assert len(data) == 2
        assert data[0]["name"] == "Alice"

    def test_custom_delimiter(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.csv")
        with open(path, "w") as f:
            f.write("name;age\nAlice;30\n")
        extractor = CSVExtractor({"path": path, "delimiter": ";"})
        data = extractor.extract()
        assert data[0]["age"] == "30"


class TestJSONFileExtractor:
    def test_list_json(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.json")
        with open(path, "w") as f:
            json.dump([{"a": 1}, {"a": 2}], f)
        extractor = JSONFileExtractor({"path": path})
        assert len(extractor.extract()) == 2

    def test_wrapped_json(self, tmp_dir):
        path = os.path.join(tmp_dir, "test.json")
        with open(path, "w") as f:
            json.dump({"data": [{"a": 1}]}, f)
        extractor = JSONFileExtractor({"path": path, "root_key": "data"})
        assert extractor.extract()[0]["a"] == 1


class TestExtractorFactory:
    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown source type"):
            ExtractorFactory.create({"type": "oracle_db"})

    def test_register_custom(self):
        from etl_core.extractors import BaseExtractor

        class FakeExtractor(BaseExtractor):
            def extract(self):
                return [{"fake": True}]

        ExtractorFactory.register("fake", FakeExtractor)
        e = ExtractorFactory.create({"type": "fake"})
        assert e.extract() == [{"fake": True}]


# ------------------------------------------------------------------ #
#  Transformer tests                                                    #
# ------------------------------------------------------------------ #

class TestRenameColumns:
    def test_rename(self, sample_data):
        t = RenameColumnsTransformer({"mapping": {"id": "user_id"}})
        result = t.transform(sample_data)
        assert "user_id" in result[0]
        assert "id" not in result[0]


class TestFilterRows:
    def test_filter(self):
        data = [{"age": "20"}, {"age": "15"}, {"age": "30"}]
        t = FilterRowsTransformer({"condition": "int(age) >= 18"})
        result = t.transform(data)
        assert len(result) == 2


class TestCastTypes:
    def test_cast(self, sample_data):
        t = CastTypesTransformer({"columns": {"id": "int", "score": "float"}})
        result = t.transform([{"id": "5", "score": "3.14"}])
        assert result[0]["id"] == 5
        assert result[0]["score"] == pytest.approx(3.14)


class TestAddColumn:
    def test_add_column(self):
        data = [{"price": 10, "qty": 3}]
        t = AddColumnTransformer({"name": "total", "expression": "price * qty"})
        result = t.transform(data)
        assert result[0]["total"] == 30


class TestDropColumns:
    def test_drop(self, sample_data):
        t = DropColumnsTransformer({"columns": ["email", "score"]})
        result = t.transform(sample_data)
        assert "email" not in result[0]
        assert "score" not in result[0]
        assert "id" in result[0]


class TestDeduplicate:
    def test_dedup_by_subset(self, sample_data):
        t = DeduplicateTransformer({"subset": ["email"]})
        result = t.transform(sample_data)
        emails = [r["email"] for r in result]
        assert len(emails) == len(set(emails))

    def test_dedup_all(self):
        data = [{"a": 1, "b": 2}, {"a": 1, "b": 2}, {"a": 3, "b": 4}]
        t = DeduplicateTransformer({})
        assert len(t.transform(data)) == 2


class TestFillNulls:
    def test_fill(self, sample_data):
        t = FillNullsTransformer({"columns": {"score": "0"}})
        result = t.transform(sample_data)
        nulls = [r for r in result if r["score"] is None]
        assert len(nulls) == 0


class TestNormalizeText:
    def test_strip_lower(self, sample_data):
        t = NormalizeTextTransformer({"columns": ["name", "email"], "operations": ["strip", "lower"]})
        result = t.transform(sample_data)
        assert result[0]["name"] == "alice"
        assert result[0]["email"] == "alice@example.com"


class TestTransformerPipeline:
    def test_pipeline_chain(self):
        data = [{"id": "1", "name": " Alice ", "score": "90"}]
        pipeline = TransformerPipeline([
            {"type": "cast_types", "columns": {"id": "int", "score": "int"}},
            {"type": "normalize_text", "columns": ["name"], "operations": ["strip"]},
            {"type": "add_column", "name": "label", "expression": "'user_' + str(id)"},
        ])
        result = pipeline.apply(data)
        assert result[0]["id"] == 1
        assert result[0]["name"] == "Alice"
        assert result[0]["label"] == "user_1"


# ------------------------------------------------------------------ #
#  Loader tests                                                         #
# ------------------------------------------------------------------ #

class TestCSVLoader:
    def test_write(self, tmp_dir):
        path = os.path.join(tmp_dir, "out.csv")
        data = [{"name": "Alice", "score": 90}]
        loader = CSVLoader({"path": path, "write_mode": "replace"})
        loader.load(data)
        assert os.path.exists(path)

    def test_append(self, tmp_dir):
        path = os.path.join(tmp_dir, "out.csv")
        loader = CSVLoader({"path": path, "write_mode": "replace"})
        loader.load([{"x": 1}])
        loader2 = CSVLoader({"path": path, "write_mode": "append"})
        loader2.load([{"x": 2}])
        with open(path) as f:
            lines = f.readlines()
        assert len(lines) == 3  # header + 2 rows


class TestJSONFileLoader:
    def test_write(self, tmp_dir):
        path = os.path.join(tmp_dir, "out.json")
        loader = JSONFileLoader({"path": path, "write_mode": "replace"})
        loader.load([{"a": 1}])
        with open(path) as f:
            data = json.load(f)
        assert data[0]["a"] == 1


# ------------------------------------------------------------------ #
#  Integration test                                                     #
# ------------------------------------------------------------------ #

class TestETLEngineIntegration:
    def test_csv_to_json(self, tmp_dir):
        # Write input CSV
        csv_path = os.path.join(tmp_dir, "input.csv")
        with open(csv_path, "w") as f:
            f.write("name,score\nAlice,90\nBob,70\n")

        out_path = os.path.join(tmp_dir, "output.json")
        config = {
            "pipeline_name": "test_pipeline",
            "source": {"type": "csv", "path": csv_path},
            "transformations": [
                {"type": "cast_types", "columns": {"score": "int"}},
                {"type": "filter_rows", "condition": "int(score) >= 80"},
            ],
            "destination": {"type": "json_file", "path": out_path, "write_mode": "replace"},
        }

        config_path = os.path.join(tmp_dir, "pipeline.json")
        with open(config_path, "w") as f:
            json.dump(config, f)

        meta_path = os.path.join(tmp_dir, "meta.duckdb")
        engine = ETLEngine(config_path, metadata_db=meta_path)
        stats = engine.run()

        assert stats["status"] == "success"
        assert stats["rows_extracted"] == 2
        assert stats["rows_loaded"] == 1  # only Alice passes filter

        with open(out_path) as f:
            result = json.load(f)
        assert result[0]["name"] == "Alice"
