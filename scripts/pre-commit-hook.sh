#!/usr/bin/env bash
# .git/hooks/pre-commit
#
# Runs ONLY fast, offline unit tests before every commit.
# Install:
#   cp scripts/pre-commit-hook.sh .git/hooks/pre-commit
#   chmod +x .git/hooks/pre-commit
#
# Skip for a one-off emergency commit:
#   git commit --no-verify -m "..."

set -e

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

echo "🔍 Running pre-commit unit tests..."

# Run only the unit-marked tests — no DB needed, runs in < 3 seconds
python -m pytest tests/test_unit.py -q --tb=short 2>&1

STATUS=$?

if [ $STATUS -ne 0 ]; then
  echo ""
  echo "❌ Pre-commit tests FAILED. Fix the issues above before committing."
  echo "   To skip (emergency only): git commit --no-verify"
  exit 1
fi

echo "✅ All pre-commit tests passed."
exit 0
