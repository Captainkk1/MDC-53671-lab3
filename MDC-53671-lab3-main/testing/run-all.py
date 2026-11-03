"""Test helpe: run the 5 scripts and fake-stations.py in one terminal, then check the results."""
import os
import subprocess
import sys
import tempfile
import time

from confluent_kafka.admin import AdminClient, NewTopic

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOPICS = ["velib-stations", "stations-status", "empty-stations"]
CONSUMERS = ["monitor-kafka.py", "stations-activity.py", "empty-stations.py",
             "alert-empty-stations.py", "archive-data.py"]
ARCHIVE = os.path.join(ROOT, "velib-stations.txt")


def create_topics():
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    futures = admin.create_topics([NewTopic(t, 1, 1) for t in TOPICS])
    for topic, future in futures.items():
        try:
            future.result()
            print(f"topic {topic} created")
        except Exception:
            print(f"topic {topic} already exists")


def count_lines(path):
    if not os.path.exists(path):
        return 0
    with open(path, encoding="utf-8") as f:
        return sum(1 for _ in f)


def read(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.read()


def main():
    create_topics()
    archive_before = count_lines(ARCHIVE)
    env = dict(os.environ, PYTHONUNBUFFERED="1")
    out_dir = tempfile.mkdtemp()

    processes = {}
    for script in CONSUMERS:
        out_path = os.path.join(out_dir, script + ".log")
        out_file = open(out_path, "w", encoding="utf-8")
        process = subprocess.Popen([sys.executable, os.path.join(ROOT, script)],
                                   cwd=ROOT, env=env, stdout=out_file,
                                   stderr=subprocess.STDOUT)
        processes[script] = (process, out_file, out_path)

    print("wait the consumers to start")
    time.sleep(3)

    print("send the fake stations")
    subprocess.run([sys.executable, os.path.join(ROOT, "testing", "fake-stations.py")],
                   cwd=ROOT, env=env)

    print("waiting for the last messages")
    time.sleep(4)

    logs = {}
    for script, (process, out_file, out_path) in processes.items():
        process.terminate()
        process.wait()
        out_file.close()
        logs[script] = read(out_path)

    checks = [
        ("stations-activity: 3 changes",
         sum("->" in line for line in logs["stations-activity.py"].splitlines()) == 3),
        ("empty-stations: 1 'became EMPTY'",
         logs["empty-stations.py"].count("became EMPTY") == 1),
        ("empty-stations: 1 'no longer empty'",
         logs["empty-stations.py"].count("is no longer empty") == 1),
        ("alert-empty-stations: exactly 1 alert",
         logs["alert-empty-stations.py"].count("Notice") == 1),
        ("archive-data: 6 new lines in the archive",
         count_lines(ARCHIVE) - archive_before == 6),
        ("monitor-kafka: sees the 3 topics",
         all(t in logs["monitor-kafka.py"] for t in TOPICS)),
    ]

    for script, log in logs.items():
        print(f"\n===== {script} =====")
        print(log.strip() or "(no output)")

    print("\n===== RESULT =====")
    for name, ok in checks:
        print(f"[{'OK' if ok else 'FAIL'}] {name}")


if __name__ == "__main__":
    main()
