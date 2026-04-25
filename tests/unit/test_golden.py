from unifysql.eval.golden import EvalResult, compare_runs, compute_em


def make_eval_result(question_id: str, ex: bool, em: bool, run_id: str) -> EvalResult:
    return EvalResult(
        question_id=question_id,
        gold_sql="SELECT * FROM users",
        gen_sql="SELECT * FROM users",
        ex=ex,
        em=em,
        latency_ms=100.0,
        token_count=50,
        error=None,
        run_id=run_id,
    )


# Run A — baseline
run_a = [
    make_eval_result("q001", ex=True, em=True, run_id="run_a"),  # passes
    make_eval_result("q002", ex=True, em=False, run_id="run_a"),  # passes EX
    make_eval_result("q003", ex=False, em=False, run_id="run_a"),  # fails
    make_eval_result("q004", ex=True, em=True, run_id="run_a"),  # passes
]

# Run B — comparison
run_b = [
    make_eval_result("q001", ex=False, em=False, run_id="run_b"),  # regressed
    make_eval_result("q002", ex=True, em=False, run_id="run_b"),  # same
    make_eval_result("q003", ex=True, em=True, run_id="run_b"),  # improved
    make_eval_result("q004", ex=True, em=True, run_id="run_b"),  # same
]


def test_compute_em() -> None:
    """Pytest unit test for computing Exact Match (EM) for SQL queries."""
    sql1 = "SELECT id FROM table;"
    sql2 = "SELECT id, name FROM table;"
    sql3 = "SELECT  id  FROM table;"

    # Test identical SQL
    assert compute_em(sql1, sql1)

    # Test different SQL
    assert not compute_em(sql1, sql2)

    # Test case insensitive
    assert compute_em(sql1, sql1.lower())

    # Test extra whitespace
    assert compute_em(sql1, sql3)


def test_compare_runs() -> None:
    """Pytest unit test for comparing two eval runs."""
    # Test 1 - q001 regressed
    regression_report = compare_runs(run_a, run_b)
    assert "q001" in regression_report.regressed_question_ids

    # Test 2 - q003 improved
    assert "q003" in regression_report.improved_question_ids

    # Test 3 - EX
    assert regression_report.run_a_ex == 0.75
    assert regression_report.run_b_ex == 0.75
    assert regression_report.ex_delta == 0.0
