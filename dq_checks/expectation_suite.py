from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Callable, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

log = logging.getLogger("dq_checks")

class DataQualityError(Exception):

@dataclass
class ExpectationResult:
    name: str
    success: bool
    severity: str
    details: str


@dataclass
class SuiteResult:
    suite_name: str
    results: list = field(default_factory=list)

    @property
    def success(self) -> bool:
        return all(r.success for r in self.results if r.severity == "error")

    @property
    def failures(self):
        return [r for r in self.results if not r.success]

    @property
    def error_failures(self):
        return [r for r in self.results if not r.success and r.severity == "error"]

    @property
    def warning_failures(self):
        return [r for r in self.results if not r.success and r.severity == "warning"]

    def summary(self) -> str:
        lines = [
            f"DQ suite '{self.suite_name}': {len(self.results)} expectations, "
            f"{len(self.failures)} failed ({len(self.error_failures)} error, "
            f"{len(self.warning_failures)} warning)"
        ]
        for r in self.results:
            status = "PASS" if r.success else ("FAIL" if r.severity == "error" else "WARN")
            lines.append(f"  [{status}] {r.name} -- {r.details}")
        return "\n".join(lines)

    def raise_if_failed(self):
        log.info(self.summary())
        if not self.success:
            failed_names = ", ".join(r.name for r in self.error_failures)
            raise DataQualityError(
                f"DQ suite '{self.suite_name}' failed {len(self.error_failures)} expectation(s): {failed_names}"
            )


class ExpectationSuite:

    def __init__(self, name: str):
        self.name = name
        self._checks: list[tuple[str, Callable[[DataFrame], ExpectationResult], str]] = []

    def _add(self, name: str, fn: Callable[[DataFrame], ExpectationResult], severity: str):
        self._checks.append((name, fn, severity))

    def expect_column_to_exist(self, column: str, severity: str = "error"):
        def check(df: DataFrame) -> ExpectationResult:
            exists = column in df.columns
            return ExpectationResult(
                name=f"expect_column_to_exist({column})",
                success=exists,
                severity=severity,
                details="present" if exists else f"missing -- columns were {df.columns}",
            )

        self._add(check.__name__ + f"::{column}", check, severity)

    def expect_column_type_to_be(self, column: str, expected_type: str, severity: str = "error"):
        def check(df: DataFrame) -> ExpectationResult:
            if column not in df.columns:
                return ExpectationResult(
                    name=f"expect_column_type_to_be({column},{expected_type})",
                    success=False,
                    severity=severity,
                    details=f"column {column} missing, cannot check type",
                )
            actual = dict(df.dtypes)[column]
            success = actual == expected_type
            return ExpectationResult(
                name=f"expect_column_type_to_be({column},{expected_type})",
                success=success,
                severity=severity,
                details=f"actual type was '{actual}'",
            )

        self._add(check.__name__ + f"::{column}", check, severity)

    def expect_column_values_to_not_be_null(self, column: str, mostly: float = 1.0, severity: str = "error"):
        def check(df: DataFrame) -> ExpectationResult:
            total = df.count()
            if total == 0:
                return ExpectationResult(
                    name=f"expect_column_values_to_not_be_null({column})",
                    success=True,
                    severity=severity,
                    details="table is empty, vacuously true",
                )
            non_null = df.filter(F.col(column).isNotNull()).count()
            ratio = non_null / total
            success = ratio >= mostly
            return ExpectationResult(
                name=f"expect_column_values_to_not_be_null({column})",
                success=success,
                severity=severity,
                details=f"{ratio:.4%} non-null (threshold {mostly:.0%}), {total - non_null} nulls out of {total}",
            )

        self._add(check.__name__ + f"::{column}", check, severity)

    def expect_column_values_to_be_between(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        mostly: float = 1.0,
        severity: str = "error",
    ):
        def check(df: DataFrame) -> ExpectationResult:
            total = df.count()
            if total == 0:
                return ExpectationResult(
                    name=f"expect_column_values_to_be_between({column})",
                    success=True,
                    severity=severity,
                    details="table is empty, vacuously true",
                )
            cond = F.lit(True)
            if min_value is not None:
                cond = cond & (F.col(column) >= min_value)
            if max_value is not None:
                cond = cond & (F.col(column) <= max_value)
            in_range = df.filter(F.col(column).isNotNull() & cond).count()
            ratio = in_range / total
            success = ratio >= mostly
            return ExpectationResult(
                name=f"expect_column_values_to_be_between({column},{min_value},{max_value})",
                success=success,
                severity=severity,
                details=f"{ratio:.4%} within [{min_value},{max_value}] (threshold {mostly:.0%})",
            )

        self._add(check.__name__ + f"::{column}", check, severity)

    def expect_column_values_to_be_in_set(
        self, column: str, value_set: set, mostly: float = 1.0, severity: str = "error"
    ):
        def check(df: DataFrame) -> ExpectationResult:
            total = df.count()
            if total == 0:
                return ExpectationResult(
                    name=f"expect_column_values_to_be_in_set({column})",
                    success=True,
                    severity=severity,
                    details="table is empty, vacuously true",
                )
            in_set = df.filter(F.col(column).isin(list(value_set))).count()
            ratio = in_set / total
            success = ratio >= mostly
            return ExpectationResult(
                name=f"expect_column_values_to_be_in_set({column})",
                success=success,
                severity=severity,
                details=f"{ratio:.4%} in {value_set} (threshold {mostly:.0%})",
            )

        self._add(check.__name__ + f"::{column}", check, severity)

    def expect_table_row_count_to_be_between(
        self, min_value: int = 0, max_value: Optional[int] = None, severity: str = "error"
    ):
        def check(df: DataFrame) -> ExpectationResult:
            total = df.count()
            success = total >= min_value and (max_value is None or total <= max_value)
            return ExpectationResult(
                name=f"expect_table_row_count_to_be_between({min_value},{max_value})",
                success=success,
                severity=severity,
                details=f"row count was {total}",
            )

        self._add(check.__name__, check, severity)

    def expect_no_exact_duplicate_rows(self, subset: list, severity: str = "warning"):
        def check(df: DataFrame) -> ExpectationResult:
            total = df.count()
            distinct = df.select(*subset).distinct().count()
            dup_count = total - distinct
            success = dup_count == 0
            return ExpectationResult(
                name=f"expect_no_exact_duplicate_rows({subset})",
                success=success,
                severity=severity,
                details=f"{dup_count} duplicate rows found on {subset}",
            )

        self._add(check.__name__ + f"::{subset}", check, severity)

    def expect_custom(
        self,
        name: str,
        fn: Callable[[DataFrame], bool],
        details_fn: Callable[[DataFrame], str] = None,
        severity: str = "error",
    ):

        def check(df: DataFrame) -> ExpectationResult:
            success = fn(df)
            details = details_fn(df) if details_fn else ("ok" if success else "custom check failed")
            return ExpectationResult(name=name, success=success, severity=severity, details=details)

        self._add(name, check, severity)

    def run(self, df: DataFrame) -> SuiteResult:
        results = []
        for name, fn, severity in self._checks:
            try:
                results.append(fn(df))
            except Exception as exc:
                results.append(
                    ExpectationResult(name=name, success=False, severity=severity, details=f"check raised: {exc}")
                )
        return SuiteResult(suite_name=self.name, results=results)
