"""Background job runner for playground (tests + model training)."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared.paths import REPO_ROOT, TEST_DIR  # noqa: E402
LOCAL_TESTS = [
    "test_demand_inventory_scenarios.py",
    "test_pricing_rl_scenarios.py",
]

INTEGRATION_TESTS = [
    "test_system.py",
    "test_procurement_agent.py",
    "test_kafka_procurement.py",
]

HEALTH_URLS = [
    "http://localhost:8001/health",
    "http://localhost:8002/health",
    "http://localhost:8003/health",
    "http://localhost:8004/health",
]


@dataclass
class JobStep:
    id: str
    label: str
    status: str = "pending"  # pending | running | pass | fail | skip
    detail: str = ""


@dataclass
class Job:
    id: str
    kind: str  # tests | train
    status: str = "queued"  # queued | running | succeeded | failed
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    started_at: str | None = None
    finished_at: str | None = None
    progress: float = 0.0
    message: str = ""
    steps: list[JobStep] = field(default_factory=list)
    log: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def append_log(self, line: str) -> None:
        with self._lock:
            self.log.append(line.rstrip("\n"))
            if len(self.log) > 2000:
                self.log = self.log[-1500:]

    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            return {
                "id": self.id,
                "kind": self.kind,
                "status": self.status,
                "created_at": self.created_at,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "progress": round(self.progress, 3),
                "message": self.message,
                "steps": [
                    {
                        "id": s.id,
                        "label": s.label,
                        "status": s.status,
                        "detail": s.detail,
                    }
                    for s in self.steps
                ],
                "log": list(self.log[-400:]),
                "summary": dict(self.summary),
            }


class JobManager:
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()
        self._active_kinds: set[str] = set()

    def get(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def list_recent(self, limit: int = 20) -> list[dict[str, Any]]:
        jobs = sorted(self._jobs.values(), key=lambda j: j.created_at, reverse=True)
        return [j.to_dict() for j in jobs[:limit]]

    def start(self, kind: str, runner: Callable[[Job], None]) -> Job:
        with self._lock:
            if kind in self._active_kinds:
                raise RuntimeError(f"Zadanie typu '{kind}' już trwa.")
            job = Job(id=str(uuid.uuid4())[:8], kind=kind)
            self._jobs[job.id] = job
            self._active_kinds.add(kind)

        def _wrap() -> None:
            job.status = "running"
            job.started_at = datetime.utcnow().isoformat() + "Z"
            try:
                runner(job)
                if job.status == "running":
                    job.status = "succeeded"
                    job.progress = 1.0
            except Exception as exc:  # noqa: BLE001
                job.status = "failed"
                job.message = str(exc)
                job.append_log(f"ERROR: {exc}")
            finally:
                job.finished_at = datetime.utcnow().isoformat() + "Z"
                with self._lock:
                    self._active_kinds.discard(kind)

        threading.Thread(target=_wrap, daemon=True).start()
        return job


job_manager = JobManager()


def _integration_ready() -> bool:
    try:
        import requests
    except ImportError:
        return False
    for url in HEALTH_URLS:
        try:
            resp = requests.get(url, timeout=3)
            if resp.status_code != 200:
                return False
        except Exception:  # noqa: BLE001
            return False
    return True


def run_tests_job(job: Job) -> None:
    scripts = list(LOCAL_TESTS)
    integration = _integration_ready()
    if integration:
        scripts.extend(INTEGRATION_TESTS)
        job.append_log("Usługi 8001–8004 dostępne — uruchamiam też testy integracyjne.")
    else:
        job.append_log("Usługi 8001–8004 niedostępne — pomijam testy integracyjne.")

    job.steps = [JobStep(id=name, label=name) for name in scripts]
    if not integration:
        for name in INTEGRATION_TESTS:
            job.steps.append(
                JobStep(id=name, label=name, status="skip", detail="services not healthy")
            )

    total = max(len(scripts), 1)
    passed = 0
    failed = 0
    skipped = len(INTEGRATION_TESTS) if not integration else 0

    for index, script in enumerate(scripts):
        step = next(s for s in job.steps if s.id == script)
        step.status = "running"
        job.message = f"Uruchamiam {script}…"
        job.progress = index / total
        job.append_log("=" * 60)
        job.append_log(f"RUNNING: {script}")

        path = TEST_DIR / script
        if not path.is_file():
            step.status = "fail"
            step.detail = "missing file"
            failed += 1
            job.append_log(f"FAIL: missing {script}")
            continue

        proc = subprocess.Popen(
            [sys.executable, "-u", str(path)],
            cwd=str(TEST_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            job.append_log(line.rstrip("\n"))
        code = proc.wait()

        if code == 0:
            step.status = "pass"
            step.detail = "exit 0"
            passed += 1
            job.append_log(f"PASS: {script}")
        else:
            step.status = "fail"
            step.detail = f"exit {code}"
            failed += 1
            job.append_log(f"FAIL: {script} (exit {code})")

        job.progress = (index + 1) / total

    job.summary = {
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "total_executed": passed + failed,
    }
    job.progress = 1.0
    if failed:
        job.status = "failed"
        job.message = f"Niepowodzenia: {failed}/{passed + failed}"
    else:
        job.status = "succeeded"
        job.message = f"Wszystkie uruchomione testy OK ({passed})"
        if skipped:
            job.message += f", pominięto {skipped} integracyjnych"


def run_train_job(job: Job) -> None:
    force = os.getenv("PLAYGROUND_FORCE_RETRAIN", "0")
    limit = os.getenv("PLAYGROUND_TOP_PRODUCTS_LIMIT", "10")

    job.steps = [
        JobStep(id="compose", label="docker-compose batch-train", status="running"),
    ]
    job.message = "Trening modeli (batch)…"
    job.progress = 0.05
    job.append_log("Uruchamianie training_service --batch-train")

    compose = shutil.which("docker-compose") or shutil.which("docker")
    if not compose:
        raise RuntimeError("Nie znaleziono docker-compose / docker w PATH.")

    if Path(compose).name == "docker":
        cmd = [
            "docker",
            "compose",
            "run",
            "--rm",
            "-e",
            f"TOP_PRODUCTS_LIMIT={limit}",
            "-e",
            f"FORCE_RETRAIN={force}",
            "-e",
            "DB_PATH=/data/ecommerce.db",
            "training_service",
            "python",
            "/app/main.py",
            "--batch-train",
        ]
    else:
        cmd = [
            "docker-compose",
            "run",
            "--rm",
            "-e",
            f"TOP_PRODUCTS_LIMIT={limit}",
            "-e",
            f"FORCE_RETRAIN={force}",
            "-e",
            "DB_PATH=/data/ecommerce.db",
            "training_service",
            "python",
            "/app/main.py",
            "--batch-train",
        ]

    job.append_log("$ " + " ".join(cmd))
    proc = subprocess.Popen(
        cmd,
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    assert proc.stdout is not None
    line_count = 0
    for line in proc.stdout:
        job.append_log(line.rstrip("\n"))
        line_count += 1
        job.progress = min(0.95, 0.05 + line_count * 0.01)
        if "train" in line.lower() or "model" in line.lower():
            job.message = line.strip()[:120] or job.message

    code = proc.wait()
    step = job.steps[0]
    if code == 0:
        step.status = "pass"
        step.detail = "exit 0"
        job.status = "succeeded"
        job.message = "Modele wytrenowane i zapisane w cache"
        job.progress = 1.0
        job.summary = {"exit_code": 0}
        job.append_log("✓ Training finished successfully")
    else:
        step.status = "fail"
        step.detail = f"exit {code}"
        job.status = "failed"
        job.message = f"Trening zakończył się kodem {code}"
        job.progress = 1.0
        job.summary = {"exit_code": code}
        job.append_log(f"✗ Training failed (exit {code})")
        raise RuntimeError(job.message)
