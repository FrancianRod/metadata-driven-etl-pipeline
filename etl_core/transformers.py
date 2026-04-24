"""
Transformers — declarative, chainable data transformations.

Each transformation is described as a dict in the YAML/JSON config:

    transformations:
      - type: rename_columns
        mapping: { old_name: new_name }

      - type: filter_rows
        condition: "age > 18"

      - type: cast_types
        columns:
          price: float
          quantity: int

      - type: add_column
        name: full_name
        expression: "first_name + ' ' + last_name"

      - type: drop_columns
        columns: [internal_id, debug_flag]

      - type: deduplicate
        subset: [email]

      - type: fill_nulls
        columns: { score: 0, category: "unknown" }

      - type: normalize_text
        columns: [name, city]
        operations: [strip, lower]

      - type: custom_python
        code: |
          for row in data:
              row['revenue'] = row['price'] * row['qty']

Adding a new transformer:
  1. Subclass BaseTransformer
  2. Implement transform(data) → list[dict]
  3. Register in TransformerFactory._registry
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  Base                                                                 #
# ------------------------------------------------------------------ #

class BaseTransformer(ABC):
    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def transform(self, data: list[dict]) -> list[dict]:
        ...


# ------------------------------------------------------------------ #
#  Implementations                                                      #
# ------------------------------------------------------------------ #

class RenameColumnsTransformer(BaseTransformer):
    def transform(self, data):
        mapping: dict = self.config["mapping"]
        return [{mapping.get(k, k): v for k, v in row.items()} for row in data]


class FilterRowsTransformer(BaseTransformer):
    """Filter rows using a Python expression. `row` is available in scope."""

    def transform(self, data):
        condition = self.config["condition"]
        result = []
        for row in data:
            try:
                if eval(condition, {"__builtins__": {}}, row):
                    result.append(row)
            except Exception as e:
                logger.warning(f"FilterRows: could not evaluate row — {e}")
        return result


class CastTypesTransformer(BaseTransformer):
    _type_map = {"int": int, "float": float, "str": str, "bool": bool}

    def transform(self, data):
        columns: dict = self.config["columns"]
        result = []
        for row in data:
            new_row = dict(row)
            for col, type_name in columns.items():
                if col in new_row and new_row[col] is not None:
                    cast_fn = self._type_map.get(type_name, str)
                    try:
                        new_row[col] = cast_fn(new_row[col])
                    except (ValueError, TypeError):
                        logger.warning(f"CastTypes: cannot cast '{col}' value '{new_row[col]}' to {type_name}")
            result.append(new_row)
        return result


class AddColumnTransformer(BaseTransformer):
    """Add a computed column using a Python expression. `row` is in scope."""

    def transform(self, data):
        name = self.config["name"]
        expression = self.config["expression"]
        result = []
        for row in data:
            new_row = dict(row)
            try:
                new_row[name] = eval(expression, {"__builtins__": {}}, new_row)
            except Exception as e:
                new_row[name] = None
                logger.warning(f"AddColumn '{name}': {e}")
            result.append(new_row)
        return result


class DropColumnsTransformer(BaseTransformer):
    def transform(self, data):
        columns: list = self.config["columns"]
        return [{k: v for k, v in row.items() if k not in columns} for row in data]


class DeduplicateTransformer(BaseTransformer):
    def transform(self, data):
        subset: list | None = self.config.get("subset")
        seen = set()
        result = []
        for row in data:
            key = tuple(row.get(col) for col in subset) if subset else tuple(sorted(row.items()))
            if key not in seen:
                seen.add(key)
                result.append(row)
        original = len(data)
        removed = original - len(result)
        if removed:
            logger.info(f"Deduplicate: removed {removed} duplicate rows.")
        return result


class FillNullsTransformer(BaseTransformer):
    def transform(self, data):
        fill_map: dict = self.config["columns"]
        result = []
        for row in data:
            new_row = dict(row)
            for col, default in fill_map.items():
                if new_row.get(col) is None:
                    new_row[col] = default
            result.append(new_row)
        return result


class NormalizeTextTransformer(BaseTransformer):
    """Apply string operations to text columns."""
    _ops = {
        "strip": str.strip,
        "lower": str.lower,
        "upper": str.upper,
        "title": str.title,
    }

    def transform(self, data):
        columns: list = self.config["columns"]
        operations: list = self.config.get("operations", ["strip"])
        result = []
        for row in data:
            new_row = dict(row)
            for col in columns:
                if col in new_row and isinstance(new_row[col], str):
                    val = new_row[col]
                    for op in operations:
                        fn = self._ops.get(op)
                        if fn:
                            val = fn(val)
                    new_row[col] = val
            result.append(new_row)
        return result


class CustomPythonTransformer(BaseTransformer):
    """
    Run arbitrary Python code.
    The variable `data` (list[dict]) is available and must be returned/modified in place.

    Example config:
        type: custom_python
        code: |
          for row in data:
              row['profit'] = row['revenue'] - row['cost']
    """

    def transform(self, data: list[dict]) -> list[dict]:
        code = self.config["code"]
        local_scope: dict[str, Any] = {"data": data}
        exec(code, {"__builtins__": {"len": len, "str": str, "int": int, "float": float,
                                      "round": round, "abs": abs, "sum": sum,
                                      "min": min, "max": max, "enumerate": enumerate,
                                      "zip": zip, "range": range}}, local_scope)
        return local_scope["data"]


# ------------------------------------------------------------------ #
#  Factory & Pipeline                                                   #
# ------------------------------------------------------------------ #

class TransformerFactory:
    _registry: dict[str, type[BaseTransformer]] = {
        "rename_columns": RenameColumnsTransformer,
        "filter_rows": FilterRowsTransformer,
        "cast_types": CastTypesTransformer,
        "add_column": AddColumnTransformer,
        "drop_columns": DropColumnsTransformer,
        "deduplicate": DeduplicateTransformer,
        "fill_nulls": FillNullsTransformer,
        "normalize_text": NormalizeTextTransformer,
        "custom_python": CustomPythonTransformer,
    }

    @classmethod
    def create(cls, transform_config: dict) -> BaseTransformer:
        t_type = transform_config.get("type")
        if t_type not in cls._registry:
            raise ValueError(
                f"Unknown transformation '{t_type}'. "
                f"Available: {list(cls._registry.keys())}"
            )
        return cls._registry[t_type](transform_config)

    @classmethod
    def register(cls, name: str, transformer_cls: type[BaseTransformer]) -> None:
        cls._registry[name] = transformer_cls


class TransformerPipeline:
    """Chain multiple transformers in sequence."""

    def __init__(self, transformation_configs: list[dict]):
        self.transformers = [TransformerFactory.create(cfg) for cfg in transformation_configs]

    def apply(self, data: list[dict]) -> list[dict]:
        for transformer in self.transformers:
            before = len(data)
            data = transformer.transform(data)
            logger.debug(f"{type(transformer).__name__}: {before} → {len(data)} rows")
        return data
