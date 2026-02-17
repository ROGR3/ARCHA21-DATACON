lint:
    uv run ruff check .
    uv run ruff format .
    uv run ty check .
    uv run pytest .

lintfix:
    uv run ruff check . --fix --unsafe-fixes
    uv run ruff format . --check --diff
    uv run ty check .
    uv run pytest .