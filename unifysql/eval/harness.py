import asyncio
import hashlib
import json
import os
import uuid
from datetime import datetime
from typing import List, Optional

import click

from unifysql.config import settings
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

# Instantiate logger
logger = get_logger()


async def run_single(
    entry: GoldenEntry,
    model_name: Optional[str],
    run_id: str,
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
            error="schema_id is None — run POST /schemas first",
            run_id=run_id,
        )

    try:
        with Span("eval_single") as span:
            # Load semantic layer
            store = SemanticLayerStore()
            semantic_layer = store.load_by_schema_id(schema_id=entry.schema_id)

            # Build context
            context_builder = ContextBuilder(model_name=model_name)
            context_result = context_builder.build_context(
                question=entry.question,
                schema_id=entry.schema_id,
            )

            # Translate
            translator = Translator(model_name=model_name)
            generated_sql = translator.translate(
                context=context_result,
                request=TranslationRequest(
                    question=entry.question,
                    schema_id=entry.schema_id,
                    dialect=entry.dialect,
                    model_preference=None,
                    execute=execute,
                    preview=preview,
                ),
            )

            # Compile
            compiler = Compiler()
            generated_compiled = compiler.compile(
                sql=generated_sql, dialect=entry.dialect, preview=preview
            )
            gold_compiled = compiler.compile(
                sql=entry.gold_sql, dialect=entry.dialect, preview=preview
            )

            # Validate
            validator = Validator()
            generated_validated = validator.validate(
                sql=generated_compiled.sql, semantic_layer=semantic_layer
            )

            # Compute EM
            em = compute_em(entry.gold_sql, generated_compiled.sql)

            # Compute EX
            ex = False
            result_hash = None
            if execute and generated_validated.valid and settings.postgres_url:
                executor = PostgresExecutor(connection_string=settings.postgres_url)
                gen_result = await executor.execute(generated_compiled.sql)
                gold_result = await executor.execute(gold_compiled.sql)

                # Hash result sets for comparison
                gen_hash = hashlib.md5(
                    json.dumps(gen_result.result_set, sort_keys=True).encode()
                ).hexdigest()
                gold_hash = hashlib.md5(
                    json.dumps(gold_result.result_set, sort_keys=True).encode()
                ).hexdigest()
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
            error=None,
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

    for entry in entries:
        result = await run_single(
            entry=entry,
            model_name=model_name,
            run_id=run_id,
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

    # Print summary
    ex_score = sum(r.ex for r in results) / len(results) if results else 0.0
    em_score = sum(r.em for r in results) / len(results) if results else 0.0
    latencies = [r.latency_ms for r in results]
    p50 = sorted(latencies)[len(latencies) // 2] if latencies else 0.0
    p95 = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0.0

    logger.info(
        "eval_completed",
        run_id=run_id,
        n_questions=len(results),
        ex=ex_score,
        em=em_score,
        p50_ms=p50,
        p95_ms=p95,
    )

    click.echo(f"\nEval Results — run_id: {run_id}")
    click.echo(f"  EX:  {ex_score:.1%}")
    click.echo(f"  EM:  {em_score:.1%}")
    click.echo(f"  p50: {p50:.0f}ms")
    click.echo(f"  p95: {p95:.0f}ms")
    click.echo(f"  Results written to {output_path}")

    return results


@click.command()
@click.option(
    "--dataset",
    type=click.Choice(["spider", "golden"]),
    required=True,
    help="Dataset to evaluate against.",
)
@click.option(
    "--n",
    default=100,
    help="Number of questions to evaluate (spider only).",
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
    dataset: str, n: int, model: Optional[str], execute: bool, compare: Optional[str]
) -> None:
    """Run UnifySQL evaluation against golden set or Spider dataset."""
    if dataset == "golden":
        entries = load_golden_set()
    else:
        with open("benchmarks/spider/train_spider.json", "r") as f:
            spider = json.load(f)
        entries = [
            GoldenEntry(
                question_id=f"spider_train_{str(i).zfill(5)}",
                question=e["question"],
                gold_sql=e["query"],
                db_id=e["db_id"],
                dialect="postgres",
            )
            for i, e in enumerate(spider[:n], 1)
        ]

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
