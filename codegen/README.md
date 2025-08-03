All commands below are executed from `codegen/`

# Setup

```
$ uv sync
```

# Generate Query Implementations

```
$ uv run rust.py
```

# Development

Use the following for developing each individual query

```
$ ./replace.sh 1a 
$ uv run rust.py --sql_dir=junk --stats_dir=junk --output_dir=junk --src_output_dir=junk
```

Use the following for testing one query implementation under `src/`

```
$ ./replace.sh 20a
$ uv run rust.py --sql_dir=junk --stats_dir=junk --output_dir=junk
```

Use the following for testing one or multiple query implementations

```
$ uv run run_query.py 1a 20a
```