"""Generates a sample server log file for testing."""
import random
from datetime import datetime, timedelta

LEVELS = ["INFO", "INFO", "INFO", "WARNING", "ERROR", "ERROR", "CRITICAL"]
SERVICES = ["auth-service", "payment-service", "user-api", "db-connector", "cache-layer"]
MESSAGES = {
    "INFO": ["Request processed successfully", "User login successful", "Cache hit", "Health check OK"],
    "WARNING": ["High memory usage detected", "Slow query detected (>500ms)", "Rate limit approaching", "Deprecated API called"],
    "ERROR": ["Database connection timeout", "Null pointer exception", "Authentication failed", "File not found"],
    "CRITICAL": ["Service crashed", "Out of memory", "Database unreachable", "Disk full"],
}

def generate(filename="server.log", lines=10000):
    start = datetime(2024, 1, 1, 0, 0, 0)
    with open(filename, "w") as f:
        for i in range(lines):
            ts = start + timedelta(seconds=random.randint(0, 31536000))
            level = random.choice(LEVELS)
            service = random.choice(SERVICES)
            msg = random.choice(MESSAGES[level])
            f.write(f"{ts.strftime('%Y-%m-%d %H:%M:%S')} [{level}] [{service}] {msg}\n")
    print(f"Generated {lines} log lines in {filename}")

if __name__ == "__main__":
    generate()
