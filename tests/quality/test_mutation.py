import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_DIR = Path(__file__).resolve().parent.parent
MUTANTS_DIR = PROJECT_DIR / "mutants"
STATS_FILE = MUTANTS_DIR / "mutmut-cicd-stats.json"
MIN_MUTATION_SCORE = 0.70


@pytest.mark.mutation
def test_mutation_score():
    if "mutants" in str(Path(__file__).resolve()):
        pytest.skip("Running inside mutants directory (mutmut copy)")

    if MUTANTS_DIR.exists():
        shutil.rmtree(MUTANTS_DIR)
    MUTANTS_DIR.mkdir(exist_ok=True)

    result = subprocess.run(
        [sys.executable, "-m", "mutmut", "run"],
        cwd=PROJECT_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=300,
    )

    assert result.returncode == 0, f"mutmut run failed (exit code {result.returncode})"

    subprocess.run(
        [sys.executable, "-m", "mutmut", "export-cicd-stats"],
        cwd=PROJECT_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=30,
    )

    assert STATS_FILE.exists(), f"Stats file {STATS_FILE} not found"
    stats = json.loads(STATS_FILE.read_text())

    killed = stats["killed"]
    survived = stats["survived"]
    no_tests = stats["no_tests"]
    total = stats["total"]
    suspicious = stats.get("suspicious", 0)
    timeout = stats.get("timeout", 0)

    covered = killed + survived + suspicious + timeout
    score = killed / covered if covered > 0 else 0.0
    overall = killed / total if total > 0 else 0.0

    print(f"\n  Total mutants:       {total}")
    print(f"  With test coverage:  {covered} ({no_tests} without tests)")
    print(f"  Killed:              {killed}")
    print(f"  Survived:            {survived}")
    print(f"  Suspicious/timeout:  {suspicious + timeout}")
    print(f"  Mutation score:      {score:.1%}  (on covered lines)")
    print(f"  Overall score:       {overall:.1%}  (on total)")

    assert score >= MIN_MUTATION_SCORE, (
        f"Mutation score {score:.1%} < threshold {MIN_MUTATION_SCORE:.0%} "
        f"(killed={killed}, survived={survived}, no_tests={no_tests})"
    )
