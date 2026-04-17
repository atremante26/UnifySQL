import json
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel


class GoldenEntry(BaseModel):
    """A single golden set entry from `Spider` dev set for regression testing."""

    question_id: str
    question: str
    gold_sql: str
    schema_id: Optional[UUID] = None
    db_id: str
    dialect: str = "postgres"


class EvalResult(BaseModel):
    """The result of running a single golden set entry through the pipeline."""

    question_id: str
    gold_sql: str
    gen_sql: str
    result_hash: Optional[str] = None
    ex: bool
    em: bool
    latency_ms: float
    token_count: int
    error: Optional[str]
    run_id: str


class RegressionReport(BaseModel):
    """
    Comparison report between two eval runs showing
    regressions and improvements.
    """

    run_a_id: str
    run_b_id: str
    regressed_question_ids: List[str]
    improved_question_ids: List[str]
    ex_delta: float
    em_delta: float
    run_a_ex: float
    run_b_ex: float
    run_a_em: float
    run_b_em: float
    timestamp: datetime


def load_golden_set(path: str = "unifysql/eval/golden_set.json") -> List[GoldenEntry]:
    """Loads the golden set from a JSON file."""
    with open(path, "r") as f:
        data = json.load(f)
    return [GoldenEntry.model_validate(entry) for entry in data]


def compute_em(gold_sql: str, gen_sql: str) -> bool:
    """Computes exact match between gold and generated SQL."""

    def normalize(sql: str) -> str:
        return " ".join(sql.lower().strip().split())

    return normalize(gold_sql) == normalize(gen_sql)


def compare_runs(run_a: List[EvalResult], run_b: List[EvalResult]) -> RegressionReport:
    """Compares two eval runs and returns a regression report."""
    # Build lookup dicts
    run_a_map = {r.question_id: r for r in run_a}
    run_b_map = {r.question_id: r for r in run_b}

    # Find common questions
    common_ids = set(run_a_map.keys()) & set(run_b_map.keys())

    regressed = []
    improved = []

    for qid in common_ids:
        a = run_a_map[qid]
        b = run_b_map[qid]

        if a.ex and not b.ex:
            regressed.append(qid)
        elif not a.ex and b.ex:
            improved.append(qid)

    # Compute overall EX/EM scores
    run_a_ex = sum(r.ex for r in run_a) / len(run_a) if run_a else 0.0
    run_b_ex = sum(r.ex for r in run_b) / len(run_b) if run_b else 0.0
    run_a_em = sum(r.em for r in run_a) / len(run_a) if run_a else 0.0
    run_b_em = sum(r.em for r in run_b) / len(run_b) if run_b else 0.0

    return RegressionReport(
        run_a_id=run_a[0].run_id if run_a else "",
        run_b_id=run_b[0].run_id if run_b else "",
        regressed_question_ids=regressed,
        improved_question_ids=improved,
        run_a_ex=run_a_ex,
        run_b_ex=run_b_ex,
        run_a_em=run_a_em,
        run_b_em=run_b_em,
        ex_delta=run_b_ex - run_a_ex,
        em_delta=run_b_em - run_a_em,
        timestamp=datetime.now(),
    )
