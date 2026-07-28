# scripts/detect_and_configure_env.py — run this once on your laptop, not in Docker

import psutil, os

total_ram_gb = psutil.virtual_memory().total / (1024**3)
total_cores  = os.cpu_count() or 2

# Reserve room for Postgres, Airflow scheduler/webserver, backend, Metabase, OS
spark_worker_mem_gb = max(1, int(total_ram_gb * 0.4))   # ~40% of laptop RAM to Spark
spark_worker_cores  = max(1, total_cores - 2)           # leave 2 cores for everything else

env_path = ".env"
lines = open(env_path).read().splitlines() if os.path.exists(env_path) else []
lines = [l for l in lines if not l.startswith(("SPARK_WORKER_MEMORY", "SPARK_WORKER_CORES"))]
lines.append(f"SPARK_WORKER_MEMORY={spark_worker_mem_gb}g")
lines.append(f"SPARK_WORKER_CORES={spark_worker_cores}")
open(env_path, "w").write("\n".join(lines) + "\n")

print(f"Detected {total_ram_gb:.1f}GB RAM, {total_cores} cores.")
print(f"Set SPARK_WORKER_MEMORY={spark_worker_mem_gb}g, SPARK_WORKER_CORES={spark_worker_cores}")