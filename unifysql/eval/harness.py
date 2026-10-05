import os

os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TRANSFORMERS_VERBOSITY"] = "error"

import asyncio
import hashlib
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import click
import sqlglot
from dotenv import load_dotenv

from unifysql.eval.golden import (
    EvalResult,
    GoldenEntry,
    compare_runs,
    compute_em,
    load_golden_set,
)
from unifysql.execution.postgres_executor import PostgresExecutor
from unifysql.observability.logger import get_logger
from unifysql.observability.tracer import Span
from unifysql.semantic.models import TranslationRequest
from unifysql.semantic.store import SemanticLayerStore
from unifysql.translation.compiler import Compiler
from unifysql.translation.context_builder import ContextBuilder
from unifysql.translation.translator import Translator
from unifysql.translation.validator import Validator

load_dotenv()

# Instantiate logger
logger = get_logger()

# Spider Golden Evaluation
SPIDER_SCHEMA_IDS_PATH = "benchmarks/spider/schema_ids.json"

# Error prefix marking questions whose gold SQL Postgres rejects (e.g. Spider's
# SQLite-style loose GROUP BY). These are benchmark defects, not model failures,
# and are excluded from the EX denominator in run_eval.
GOLD_SQL_INVALID = "gold_sql_invalid_for_postgres"

# Error prefix marking questions skipped because their database has no
# registered schema (e.g. excluded databases like car_1). Nothing ran, so
# run_eval excludes them from both the EX and EM denominators.
SKIPPED_NO_SCHEMA = "skipped_no_schema_id"

SPIDER_QUESTION_FILES = {
    "spider": ("benchmarks/spider/train_spider.json", "spider_train"),
    "dev": ("benchmarks/spider/dev.json", "spider_dev"),
}


def _spider_connection_string(db_id: str) -> str:
    """
    Connection string for a Spider database loaded by load_spider.sh
    (databases are named `spider_<db_id>` lowercased, on host port 5433).
    """
    return f"postgresql://postgres:postgres@localhost:5433/spider_{db_id.lower()}"


def _load_spider_schema_ids() -> Dict[str, str]:
    """
    Loads the `db_id` -> `schema_id` mapping written by
    `benchmarks/spider/scripts/register_and_populate.py`.
    """
    if not os.path.exists(SPIDER_SCHEMA_IDS_PATH):
        return {}
    with open(SPIDER_SCHEMA_IDS_PATH, "r") as f:
        return json.load(f)


def _hash_result_set(result_set: Dict[str, List[Any]]) -> str:
    """
    Canonically hashes a result set for EX comparison.
    Reconstructs rows from column-oriented format, sorts them,
    and hashes to handle row order differences between queries.
    """
    rows = [list(row_values) for row_values in zip(*result_set.values())]
    rows.sort(key=lambda r: json.dumps(r, sort_keys=True))
    return hashlib.md5(json.dumps(rows, sort_keys=True).encode()).hexdigest()


def _normalize_gold_sql(sql: str) -> str:
    """
    Normalizes Spider gold SQL for Postgres execution.
    """
    sql = sql.replace('"', "'")
    return (
        sqlglot.transpile(sql=sql, read="sqlite", write="postgres")[0]
        .strip()
        .rstrip(";")
    )


async def run_single(
    entry: GoldenEntry,
    run_id: str,
    store: SemanticLayerStore,
    context_builder: ContextBuilder,
    translator: Translator,
    compiler: Compiler,
    validator: Validator,
    execute: bool = False,
    preview: bool = True,
) -> EvalResult:
    """
    Runs a single golden set entry through the full online pipeline.
    """
    # Guard against missing schema_id
    if entry.schema_id is None:
        logger.warning("skipping_entry_no_schema_id", question_id=entry.question_id)
        return EvalResult(
            question_id=entry.question_id,
            gold_sql=entry.gold_sql,
            gen_sql="",
            ex=False,
            em=False,
            latency_ms=0.0,
            token_count=0,
            error=f"{SKIPPED_NO_SCHEMA}: run register_and_populate.py first",
            run_id=run_id,
        )

    try:
        with Span("eval_single") as span:
            # Load semantic layer
            semantic_layer = store.load_by_schema_id(schema_id=entry.schema_id)

            # Build context
            context_result = context_builder.build_context(
                question=entry.question,
                schema_id=entry.schema_id,
            )

            # Translate
            exec_preview = False if execute else preview
            generated_sql = translator.translate(
                context=context_result,
                request=TranslationRequest(
                    question=entry.question,
                    schema_id=entry.schema_id,
                    dialect=entry.dialect,
                    model_preference=None,
                    execute=execute,
                    preview=exec_preview,
                ),
            )

            # Compile
            generated_compiled = compiler.compile(
                sql=generated_sql, dialect=entry.dialect, preview=exec_preview
            )

            # Validate
            generated_validated = validator.validate(
                sql=generated_compiled.sql, semantic_layer=semantic_layer
            )

            if not generated_validated.valid:
                logger.warning(
                    "eval_validation_failed",
                    question_id=entry.question_id,
                    error_type=generated_validated.error_type,
                )

            # Compute EM
            em = compute_em(entry.gold_sql, generated_compiled.sql, entry.dialect)

            # Compute EX
            ex = False
            result_hash = None
            error = None
            if execute and generated_validated.valid:
                conn_str = _spider_connection_string(entry.db_id)
                executor = PostgresExecutor(connection_string=conn_str)

                # Execute gold first: if Postgres rejects it the question is
                # unwinnable, regardless of what the model generated.
                try:
                    gold_result = await executor.execute(
                        _normalize_gold_sql(entry.gold_sql)
                    )
                except Exception as e:
                    gold_result = None
                    error = f"{GOLD_SQL_INVALID}: {e}"

                if gold_result is not None:
                    try:
                        gen_result = await executor.execute(generated_compiled.sql)
                    except Exception as e:
                        # Generated SQL failing to execute is a model failure:
                        # keep ex=False and record why.
                        gen_result = None
                        error = f"generated SQL execution failed: {e}"

                    if gen_result is not None:
                        # Hash result sets and compare
                        gen_hash = _hash_result_set(result_set=gen_result.result_set)
                        gold_hash = _hash_result_set(result_set=gold_result.result_set)
                        ex = gen_hash == gold_hash
                        result_hash = gen_hash

        logger.info(
            "eval_single_completed",
            question_id=entry.question_id,
            ex=ex,
            em=em,
            latency_ms=span.latency_ms,
        )

        return EvalResult(
            question_id=entry.question_id,
            gold_sql=entry.gold_sql,
            gen_sql=generated_compiled.sql,
            result_hash=result_hash,
            ex=ex,
            em=em,
            latency_ms=span.latency_ms,
            token_count=0,  # TODO: aggregate token count across pipeline stages
            error=error,
            run_id=run_id,
        )

    except Exception as e:
        logger.error("eval_single_failed", question_id=entry.question_id, error=str(e))
        return EvalResult(
            question_id=entry.question_id,
            gold_sql=entry.gold_sql,
            gen_sql="",
            ex=False,
            em=False,
            latency_ms=0.0,
            token_count=0,
            error=str(e),
            run_id=run_id,
        )


async def run_eval(
    entries: List[GoldenEntry],
    model_name: Optional[str],
    execute: bool = False,
    preview: bool = True,
) -> List[EvalResult]:
    """
    Runs the full eval set through the pipeline and writes results to disk.
    """
    run_id = str(uuid.uuid4())
    results = []
    store = SemanticLayerStore()
    context_builder = ContextBuilder(model_name=model_name)
    translator = Translator(model_name=model_name)
    compiler = Compiler()
    validator = Validator()

    for entry in entries:
        result = await run_single(
            entry=entry,
            run_id=run_id,
            store=store,
            context_builder=context_builder,
            translator=translator,
            compiler=compiler,
            validator=validator,
            execute=execute,
            preview=preview,
        )
        results.append(result)

    # Write results to disk
    os.makedirs("eval/results", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"eval/results/{timestamp}_{run_id}.json"
    with open(output_path, "w") as f:
        json.dump([r.model_dump(mode="json") for r in results], f, indent=2)

    # Print summary. Questions skipped for a missing schema never ran, so they
    # leave both denominators; questions whose gold SQL cannot execute on
    # Postgres are excluded from the EX denominator only (unwinnable), since
    # EM needs no execution.
    ran = [
        r for r in results if not (r.error and r.error.startswith(SKIPPED_NO_SCHEMA))
    ]
    n_skipped = len(results) - len(ran)
    ex_scorable = [
        r for r in ran if not (r.error and r.error.startswith(GOLD_SQL_INVALID))
    ]
    n_gold_invalid = len(ran) - len(ex_scorable)
    ex_score = sum(r.ex for r in ex_scorable) / len(ex_scorable) if ex_scorable else 0.0
    em_score = sum(r.em for r in ran) / len(ran) if ran else 0.0
    latencies = [r.latency_ms for r in ran]
    p50 = sorted(latencies)[len(latencies) // 2] if latencies else 0.0
    p95 = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0.0

    logger.info(
        "eval_completed",
        run_id=run_id,
        n_questions=len(results),
        n_skipped_no_schema=n_skipped,
        n_gold_invalid=n_gold_invalid,
        ex=ex_score,
        em=em_score,
        p50_ms=p50,
        p95_ms=p95,
    )

    click.echo(f"\nEval Results — run_id: {run_id}")
    click.echo(f"  EX:  {ex_score:.1%}")
    if n_gold_invalid:
        click.echo(
            f"       ({n_gold_invalid} question(s) excluded: "
            "gold SQL not executable on Postgres)"
        )
    if n_skipped:
        click.echo(
            f"       ({n_skipped} question(s) skipped: database not registered)"
        )
    click.echo(f"  EM:  {em_score:.1%}")
    click.echo(f"  p50: {p50:.0f}ms")
    click.echo(f"  p95: {p95:.0f}ms")
    click.echo(f"  Results written to {output_path}")

    return results


@click.command()
@click.option(
    "--dataset",
    type=click.Choice(["spider", "dev", "golden"]),
    required=True,
    help="Dataset to evaluate against: Spider train split, Spider dev split, "
    "or the curated golden set.",
)
@click.option(
    "--n",
    default=100,
    help="Number of questions to evaluate (spider/dev only; 0 = all).",
)
@click.option(
    "--db",
    "dbs",
    multiple=True,
    default=(),
    help="Restrict to these db_ids (repeatable).",
)
@click.option(
    "--model",
    default=None,
    help="LLM model name override.",
)
@click.option(
    "--execute",
    is_flag=True,
    default=False,
    help="Execute generated SQL for EX computation.",
)
@click.option(
    "--compare",
    default=None,
    help="Path to previous run JSON for regression comparison.",
)
def eval_cmd(
    dataset: str,
    n: int,
    dbs: tuple,
    model: Optional[str],
    execute: bool,
    compare: Optional[str],
) -> None:
    """Run UnifySQL evaluation against golden set or Spider dataset."""
    if dataset == "golden":
        entries = load_golden_set()
    else:
        schema_ids = _load_spider_schema_ids()
        question_file, id_prefix = SPIDER_QUESTION_FILES[dataset]
        with open(question_file, "r") as f:
            spider = json.load(f)
        if n:
            spider = spider[:n]
        entries = [
            GoldenEntry(
                question_id=f"{id_prefix}_{str(i).zfill(5)}",
                question=e["question"],
                gold_sql=e["query"],
                schema_id=schema_ids.get(e["db_id"]),
                db_id=e["db_id"],
                dialect="postgres",
            )
            for i, e in enumerate(spider, 1)
        ]
        unregistered = sorted({e.db_id for e in entries if e.schema_id is None})
        if unregistered:
            click.echo(
                f"WARNING: {len(unregistered)} database(s) missing from "
                f"{SPIDER_SCHEMA_IDS_PATH}; their questions will be skipped: "
                f"{', '.join(unregistered)}. "
                "Run benchmarks/spider/scripts/register_and_populate.py first."
            )

    if dbs:
        entries = [e for e in entries if e.db_id in dbs]
        click.echo(f"Filtered to {len(entries)} question(s) from: {', '.join(dbs)}")

    results = asyncio.run(
        run_eval(
            entries=entries,
            model_name=model,
            execute=execute,
        )
    )

    if compare:
        with open(compare, "r") as f:
            prev_results = [EvalResult.model_validate(r) for r in json.load(f)]
        report = compare_runs(prev_results, results)
        click.echo(f"\nRegression Report vs {compare}")
        click.echo(f"  EX delta: {report.ex_delta:+.1%}")
        click.echo(f"  EM delta: {report.em_delta:+.1%}")
        click.echo(f"  Regressed: {len(report.regressed_question_ids)}")
        click.echo(f"  Improved:  {len(report.improved_question_ids)}")


if __name__ == "__main__":
    eval_cmd()
