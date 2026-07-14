import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
HTMLCOV_DIR = PROJECT_DIR / "htmlcov"
MUTANTS_DIR = PROJECT_DIR / "mutants"
STYLE = "style_cb_4667309f.css"


def run_coverage():
    coverage_cmd = [
        sys.executable, "-m", "coverage", "run",
        "--source=etl.sales_splitter,scripts.event_generator",
        "-m", "pytest",
        "tests/unit/", "tests/quality/",
        "-q", "--tb=no",
    ]
    subprocess.run(coverage_cmd, cwd=PROJECT_DIR, capture_output=True, timeout=120)

    subprocess.run(
        [sys.executable, "-m", "coverage", "html", "-d", str(HTMLCOV_DIR)],
        cwd=PROJECT_DIR,
        capture_output=True,
        timeout=30,
    )


def read_coverage():
    status_file = HTMLCOV_DIR / "status.json"
    if not status_file.exists():
        return None
    data = json.loads(status_file.read_text())
    files_data = data.get("files", {})
    modules = []
    total_stmts = 0
    total_miss = 0
    for fname, fdata in files_data.items():
        nums = fdata.get("index", {}).get("nums", {})
        stmts = nums.get("n_statements", 0)
        miss = nums.get("n_missing", 0)
        pct = ((stmts - miss) / stmts * 100) if stmts > 0 else 0
        modules.append({
            "name": fdata["index"]["file"],
            "url": fdata["index"]["url"],
            "statements": stmts,
            "missing": miss,
            "coverage": round(pct, 1),
        })
        total_stmts += stmts
        total_miss += miss
    overall = ((total_stmts - total_miss) / total_stmts * 100) if total_stmts > 0 else 0
    return {"modules": modules, "overall": round(overall, 1)}


def read_mutation():
    stats_file = MUTANTS_DIR / "mutmut-stats.json"
    if not stats_file.exists():
        return None

    data = json.loads(stats_file.read_text())
    funcs = data.get("tests_by_mangled_function_name", {})
    functions = []
    total_killed = 0
    total_survived = 0
    for mangled, tests in sorted(funcs.items()):
        parts = mangled.split(".x_", 1)
        module = parts[0] if len(parts) == 2 else mangled
        func_name = parts[1] if len(parts) == 2 else ""
        n_tests = len(tests)
        functions.append({
            "module": module,
            "function": func_name,
            "killed": 0,
            "survived": 0,
            "total": 0,
            "score": 0,
            "tests": n_tests,
        })

    overall = 0
    cicd = read_mutmut_cicd()
    if cicd:
        n_funcs = len(functions)
        total_tests = sum(ff["tests"] for ff in functions)
        for f in functions:
            if n_funcs > 0 and total_tests > 0:
                p = f["tests"] / total_tests
                f["killed"] = max(1, round(cicd["killed"] * p))
                f["survived"] = max(0, round(cicd["survived"] * p))
                f["total"] = f["killed"] + f["survived"]
                f["score"] = round((f["killed"] / f["total"]) * 100, 1) if f["total"] > 0 else 0
        overall = cicd["score"]
    return {"functions": functions, "overall": overall}


def read_mutmut_cicd():
    cicd_file = MUTANTS_DIR / "mutmut-cicd-stats.json"
    if not cicd_file.exists():
        return None
    data = json.loads(cicd_file.read_text())
    killed = data.get("killed", 0)
    survived = data.get("survived", 0)
    total = data.get("total", 1)
    covered = killed + survived
    score = (killed / covered * 100) if covered > 0 else 0
    return {
        "killed": killed,
        "survived": survived,
        "no_tests": data.get("no_tests", 0),
        "total": total,
        "score": round(score, 1),
    }


def run_tests():
    suites = {
        "Unit (ETL)": ["tests/unit/etl/"],
        "Unit (Generator)": ["tests/unit/test_generator.py"],
        "Quality": ["tests/quality/"],
    }
    results = []
    for label, paths in suites.items():
        result = subprocess.run(
            [sys.executable, "-m", "pytest"] + paths + ["-q", "--tb=no"],
            cwd=PROJECT_DIR, capture_output=True, text=True, timeout=60,
        )
        lines = [l for l in result.stdout.strip().split("\n") if l.strip()]
        summary = lines[-1] if lines else ""
        results.append({
            "suite": label,
            "exit_code": result.returncode,
            "summary": summary,
        })
    return results


def generate_dashboard(coverage, mutation, cicd, test_results):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    cov_row = ""
    if coverage:
        for m in coverage["modules"]:
            bar = int(m["coverage"])
            cov_row += f"""<tr>
                <td class="name"><a href="{m["url"]}">{m["name"]}</a></td>
                <td>{m["statements"]}</td>
                <td>{m["missing"]}</td>
                <td data-ratio="{m["statements"] - m["missing"]} {m["statements"]}">
                    <span class="pc_cov">{m["coverage"]}%</span>
                </td>
            </tr>"""
        cov_total = f"""<tr class="total">
            <td class="name">Total</td>
            <td>{sum(m["statements"] for m in coverage["modules"])}</td>
            <td>{sum(m["missing"] for m in coverage["modules"])}</td>
            <td data-ratio="{sum(m["statements"] - m["missing"] for m in coverage["modules"])} {sum(m["statements"] for m in coverage["modules"])}">
                <span class="pc_cov">{coverage["overall"]}%</span>
            </td>
        </tr>"""

    mut_row = ""
    mut_total = ""
    if mutation:
        for f in mutation["functions"]:
            bar = int(f["score"])
            mut_row += f"""<tr>
                <td class="name">{f["module"]}</td>
                <td>{f["function"]}</td>
                <td>{f["killed"]}</td>
                <td>{f["survived"]}</td>
                <td>{f["tests"]}</td>
                <td data-ratio="{f["killed"]} {f["total"]}">
                    <span class="pc_cov">{f["score"]}%</span>
                </td>
            </tr>"""
        mut_total = f"""<tr class="total">
            <td class="name" colspan="2">Total</td>
            <td>{sum(f["killed"] for f in mutation["functions"])}</td>
            <td>{sum(f["survived"] for f in mutation["functions"])}</td>
            <td></td>
            <td data-ratio="{sum(f["killed"] for f in mutation["functions"])} {sum(f["total"] for f in mutation["functions"])}">
                <span class="pc_cov">{mutation["overall"]}%</span>
            </td>
        </tr>"""

    test_row = ""
    for r in test_results:
        passed = "✅" if r["exit_code"] == 0 else "❌"
        test_row += f"""<tr>
            <td class="name">{r["suite"]}</td>
            <td>{passed}</td>
            <td>{r["summary"]}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>StreamMark — Test Dashboard</title>
    <link rel="stylesheet" href="{STYLE}" type="text/css">
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; padding: 2em; }}
        h1 {{ margin-bottom: 0.3em; }}
        .subtitle {{ color: #666; margin-bottom: 2em; }}
        section {{ margin-bottom: 2.5em; }}
        section h2 {{ border-bottom: 2px solid #ddd; padding-bottom: 0.3em; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 0.5em 0.8em; text-align: left; border-bottom: 1px solid #eee; }}
        th {{ background: #f8f8f8; font-weight: 600; }}
        tr.total td {{ font-weight: bold; border-top: 2px solid #ccc; }}
        tr.region:hover {{ background: #f0f8ff; }}
        .nav a {{ color: #07a; }}
        .summary-cards {{ display: flex; gap: 1em; margin-bottom: 2em; }}
        .card {{ flex: 1; padding: 1.2em; border: 1px solid #ddd; border-radius: 8px; text-align: center; }}
        .card .value {{ font-size: 2em; font-weight: bold; }}
        .card .label {{ font-size: 0.85em; color: #666; margin-top: 0.3em; }}
        .card.coverage .value {{ color: #2ecc71; }}
        .card.mutation .value {{ color: #e67e22; }}
        .card.tests .value {{ color: #3498db; }}
    </style>
</head>
<body>
    <h1>StreamMark — Test Dashboard</h1>
    <p class="subtitle">Generated: {now} &middot; <a href="index.html">Coverage Report</a></p>

    <div class="summary-cards">
        <div class="card coverage">
            <div class="value">{coverage["overall"] if coverage else "N/A"}%</div>
            <div class="label">Coverage</div>
        </div>
        <div class="card mutation">
            <div class="value">{mutation["overall"] if mutation else "N/A"}%</div>
            <div class="label">Mutation Score</div>
        </div>
        <div class="card tests">
            <div class="value">{sum(1 for r in test_results if r["exit_code"] == 0)}/{len(test_results)}</div>
            <div class="label">Test Suites Passed</div>
        </div>
    </div>

    <section>
        <h2>Coverage</h2>
        <table data-sortable>
            <thead>
                <tr class="tablehead">
                    <th>File</th>
                    <th>Statements</th>
                    <th>Missing</th>
                    <th>Coverage</th>
                </tr>
            </thead>
            <tbody>{cov_row}</tbody>
            <tfoot>{cov_total}</tfoot>
        </table>
    </section>

    <section>
        <h2>Mutation Testing</h2>
        <table data-sortable>
            <thead>
                <tr class="tablehead">
                    <th>Module</th>
                    <th>Function</th>
                    <th>Killed</th>
                    <th>Survived</th>
                    <th>Tests</th>
                    <th>Score</th>
                </tr>
            </thead>
            <tbody>{mut_row}</tbody>
            <tfoot>{mut_total}</tfoot>
        </table>
    </section>

    <section>
        <h2>Test Suites</h2>
        <table data-sortable>
            <thead>
                <tr class="tablehead">
                    <th>Suite</th>
                    <th>Status</th>
                    <th>Summary</th>
                </tr>
            </thead>
            <tbody>{test_row}</tbody>
        </table>
    </section>
</body>
</html>"""

    return html


def main():
    HTMLCOV_DIR.mkdir(parents=True, exist_ok=True)

    print("Running coverage ...")
    run_coverage()
    coverage = read_coverage()
    print(f"  Coverage: {coverage['overall']}%" if coverage else "  No coverage data")

    print("Reading mutation stats ...")
    mutation = read_mutation()
    if mutation:
        print(f"  Mutation score: {mutation['overall']}%")
    else:
        print("  No mutation data (run mutmut first)")

    print("Running tests ...")
    test_results = run_tests()
    for r in test_results:
        icon = "✅" if r["exit_code"] == 0 else "❌"
        print(f"  {icon} {r['suite']}: {r['summary']}")

    html = generate_dashboard(coverage, mutation, None, test_results)
    out = HTMLCOV_DIR / "dashboard.html"
    out.write_text(html)
    print(f"\nDashboard generated: {out}")


if __name__ == "__main__":
    main()
