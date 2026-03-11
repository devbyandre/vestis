# Contributing to Vestis

Thank you for considering a contribution! Here's how to get set up and what to expect.

## Getting Started

```bash
git clone https://github.com/YOUR_USERNAME/vestis.git
cd vestis
pip install -r requirements.txt
```

## Running Tests

```bash
# Fast unit tests (no infrastructure)
pytest tests/test_unit.py -v

# Full suite
pytest
```

## Pull Request Guidelines

- **One concern per PR.** Bug fixes and features should be separate.
- **Add tests** for any new logic in `middleware.py` or `db_utils.py`.
- **No secrets** — double-check that `.env` and any credentials are not committed.
- **Keep `config.json` clean** — it should never contain tokens or passwords.

## Reporting Bugs

Please open a GitHub Issue with:
- Steps to reproduce
- Expected vs actual behaviour
- Your environment (OS, Docker version, Python version)

## Suggesting Features

Open an issue with the `enhancement` label and describe the use case.
