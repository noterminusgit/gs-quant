# Phase 0: Coverage Infrastructure Setup

## 0.1 Create `.coveragerc` at project root
```ini
[run]
branch = True
source = gs_quant
omit =
    gs_quant/target/*
    gs_quant/content/*
    gs_quant/documentation/*
    gs_quant/_version.py
    gs_quant/test/*
    */versioneer.py

[report]
exclude_lines =
    pragma: no cover
    def __repr__
    if __name__ == .__main__
    raise NotImplementedError
    raise AssertionError
    pass
    \.\.\.
show_missing = True
fail_under = 0
precision = 2

[html]
directory = htmlcov
```

## 0.2 Add pytest config to `pyproject.toml`
```toml
[tool.pytest.ini_options]
testpaths = ["gs_quant/test"]
addopts = "--cov=gs_quant --cov-config=.coveragerc --cov-report=term-missing --cov-report=html -rsx"
asyncio_mode = "auto"
```

## 0.3 Run baseline coverage and record starting number
