# Generic Python Code Practices

Rules derived from the patterns consistently applied in this codebase. Apply these to any Python project.

---

## 1. Module & Package Organisation

- **Separate concerns into distinct layers.** Pure business logic must not import orchestration or framework dependencies (e.g., Temporal, FastAPI). Orchestration wrappers are thin and delegate to the pure layer.
- **Each package exposes a public API via `__init__.py`.** Every subpackage re-exports its public symbols so callers import from the package, not the inner file:
  ```python
  # Good
  from app.auth import MyClient
  # Bad
  from app.auth.client import MyClient
  ```
- **One responsibility per file.** Name files after the role they play, not after the class they contain. Typical roles:
  - `client.py` — external connection / API client
  - `handler.py` — request handling / preflight logic
  - `models.py` — data models (Pydantic or dataclasses)
  - `utils.py` — pure, stateless utility functions
  - `filters.py` — data filtering / pruning logic
  - `constants.py` — module-level constants
  - `errors.py` — custom exception hierarchy
  - `factory.py` — object instantiation logic
  - `ops.py` — low-level I/O operations
- **Keep infrastructure-specific files out of the core logic layer.** SQL files, YAML templates, and config files live in dedicated subdirectories (`sql/`, `templates/`, `components/`) — not inside the Python modules that use them.

---

## 2. Naming Conventions

- **Classes: PascalCase with a role suffix** that communicates what the class is: `*Client`, `*Handler`, `*Transformer`, `*Workflow`, `*Activities`, `*Error`. This makes the purpose of a class immediately obvious at the call site.
- **Functions and variables: snake_case.**
- **Boolean predicates start with `is_` or `can_`** — e.g., `is_ready()`, `can_connect()`.
- **Constants: UPPER_SNAKE_CASE** — defined at module level, never inline.
- **Private attributes: leading underscore** — e.g., `_cache`, `_client`. Signals internal state not part of the public contract.
- **Do not abbreviate names** unless the abbreviation is universally understood (e.g., `db`, `id`, `url`). `workflow_args` not `wf_args`; `connection_qualified_name` not `cqn`.

---

## 3. Functional vs Class-Based Style

**Default to functions. Use classes only when you need them.**

### When to use plain functions
- **Pure computation** — takes inputs, returns outputs, no side effects beyond logging. Examples: string manipulation, data filtering, SQL placeholder injection, config parsing.
- **I/O operations on local data** — read files, transform in memory, write back. The function is self-contained and has no lifecycle to manage.
- **Stateless helpers** — formatting, validation, predicate checks.

Functions in the logic layer (`utils.py`, `filters.py`, `enrichments.py`) must be callable from a plain Python script or unit test with zero framework imports.

### When to use classes
- **Framework requires it** — base classes from SDKs, workflow engines, or web frameworks that demand inheritance for decorator/lifecycle hooks to work (e.g., `@workflow.defn`, `QueryBasedTransformer`).
- **Resource lifecycle management** — when setup/teardown must be paired (e.g., database connections, temp directories). Use context managers (`__enter__`/`__exit__`) to scope the resource:
  ```python
  class DBConnectionManager:
      def __enter__(self):
          self._conn = db.connect(":memory:")
          return self._conn
      def __exit__(self, *exc):
          self._conn.close()
  ```
- **Facade over a stateful subsystem** — when multiple operations share a lazily-initialised backend (e.g., object store with cached connections per provider). Use `@classmethod` and class-level caches; avoid instance state where possible.
- **Data modelling** — Pydantic `BaseModel` or `@dataclass` for structured configuration, API payloads, or result containers.

### What to avoid
- **Classes used purely as namespaces for static methods.** If no method accesses `self` or `cls`, it should be a module-level function.
- **God classes** — a class that owns both business logic and orchestration. Split it: orchestration wrapper delegates to pure functions.
- **Mutable instance state that outlives a single operation.** Classes used in this codebase (transformer, handler) set their configuration at `__init__` and never mutate it during execution. If a method temporarily mutates state, it must restore it in a `finally` block.

---

## 4. Dependency Injection

**Inject dependencies through the narrowest channel that the framework allows.**

### Class-attribute injection (for framework-driven instantiation)
When a framework (Temporal, FastAPI) controls object creation, inject dependencies as **class attributes** that the framework reads at registration time:
```python
class MyActivities(BaseActivities):
    sql_client_class = MySQLClient      # Framework uses this to instantiate
    handler_class = MyHandler           # Framework uses this to instantiate
```
This is the preferred pattern when you don't control `__init__` arguments.

### Constructor injection (for application-level wiring)
When the application itself assembles components, inject **type references** via the constructor. The top-level application class accepts optional class types, not instances:
```python
class MyApplication(BaseApplication):
    def __init__(
        self,
        name: str,
        client_class: Optional[Type[BaseClient]] = None,
        handler_class: Optional[Type[BaseHandler]] = None,
    ):
```
This keeps instantiation deferred — the application creates instances when needed with the right context.

### Method-parameter injection (for async initialisation)
When a dependency requires runtime data to initialise (credentials, connection info), inject the data through a dedicated `load()` or `setup()` method, not the constructor:
```python
class MySQLClient(BaseClient):
    async def load(self, credentials: Dict[str, Any]) -> None:
        # Build engine from credentials at runtime
```
This separates construction (cheap, sync) from initialisation (expensive, async, may fail).

### Protocol-based injection (for pure logic with external dependencies)
When a pure function in the logic layer needs to call an external service, define a `Protocol` to describe what it needs. Do not import the concrete client:
```python
class SQLClient(Protocol):
    async def get_results(self, query: str) -> Any: ...

async def enrich_data(raw_path: str, sql_client: SQLClient) -> None:
    results = await sql_client.get_results(query)
```
This keeps the logic layer decoupled and testable with any mock that satisfies the protocol.

### What to avoid
- **Importing and directly instantiating concrete dependencies inside business logic.** This creates tight coupling and makes testing hard.
- **Service locators or global registries.** Pass dependencies explicitly.
- **DI containers or autowiring frameworks.** The codebase uses simple constructor/attribute injection. Keep it simple.

---

## 5. Coupling

### Loose coupling (the default)
- **Logic layer knows nothing about orchestration.** Functions in `extract/`, `filters/`, `utils/` have zero imports from Temporal, FastAPI, or any SDK. They accept plain Python types (strings, dicts, paths, Protocols) and return plain Python types.
- **Orchestration layer depends on the logic layer, never the reverse.** Activities import from `extract/`; `extract/` never imports from `pipeline/`.
- **Third-party libraries are isolated behind internal facades.** Application code imports from `app.objectstore`, `app.observability` — never directly from `obstore`, `opentelemetry`, or SDK observability modules. One internal module wraps the external dependency; all other code uses the wrapper.
- **Strategies (e.g., AU vs IS) are separate classes sharing a base.** They do not import from each other. Shared logic lives in the common base class or in the pure function layer.
- **Configuration flows one direction: inward.** Config is parsed at the boundary (Pydantic model at activity entry point), then passed as typed values to inner functions. Inner functions never reach outward to read config themselves.

### Intentional tight coupling (acceptable where it's earned)
- **Pipeline configuration constants** (timeouts, thresholds) are directly imported by workflows and activities. Config is stable and acts as a single source of truth — indirection would add complexity for no benefit.
- **A client class tightly couples to its driver** (e.g., SQLAlchemy). The driver is the client's reason to exist. Wrapping it further would be pointless abstraction.
- **Framework base classes** couple tightly to the framework. Activities, workflows, and application classes inherit from SDK/framework bases. This is the cost of using the framework and is acceptable.

### How to assess coupling
Before introducing a dependency between two modules, ask:
1. Can the importing module be unit-tested without the imported module? If not, the coupling is too tight.
2. Would a change in the imported module require changes in the importing module? If yes, consider an interface/protocol.
3. Is the imported module stable (constants, config, data models)? Tight coupling to stable modules is fine.

---

## 6. Composition over Inheritance

### Use inheritance only when
- **A framework requires it** — the base class provides lifecycle hooks, decorator registration, or serialisation support that subclasses must participate in (e.g., `@workflow.defn`, `BaseSQLClient`).
- **There is a genuine is-a relationship** with shared behaviour that varies by subclass (e.g., AU transformer vs IS transformer both extend a common `QueryBasedTransformer`).

### Prefer composition when
- **Behaviour can be delegated.** Activities don't contain business logic — they delegate to pure functions. The activity *has* a reference to the logic; it *is not* the logic.
- **A subsystem is reusable across unrelated classes.** The object store facade is composed into activities and handlers via method calls, not inheritance.
- **You need to combine capabilities.** If a class needs both SQL access and file I/O, compose those as injected dependencies rather than creating a multi-inheritance diamond.

### Pattern: Thin inheriting shell + composed pure logic
```python
# Orchestration layer: inherits from framework base, composes logic
class MyActivities(BaseActivities):
    def fetch_data(self, args):
        sql = inject_filters(self.query, args)      # Pure function
        result = self.sql_client.execute(sql)        # Composed dependency
        return filter_invalid(result, args)          # Pure function

# Logic layer: no inheritance, no framework dependencies
def inject_filters(sql: str, args: Dict) -> str: ...
def filter_invalid(data: DataFrame, args: Dict) -> DataFrame: ...
```

### What to avoid
- **Deep inheritance hierarchies** (more than 2 levels of app-owned classes). If you're overriding methods 3 layers deep, refactor to composition.
- **Inheriting from a class just to reuse one method.** Extract that method to a standalone function instead.
- **Multiple inheritance** across unrelated concerns. Use composition.

---

## 7. Immutability & Side Effects

### Pure functions: the default for business logic
- **Functions in the logic layer take inputs and return outputs.** They do not mutate their arguments:
  ```python
  # Good — returns new DataFrame
  def apply_filter(df: DataFrame, mask: Series) -> DataFrame:
      return DataFrame(df[mask])

  # Bad — mutates caller's data
  def apply_filter(df: DataFrame, mask: Series) -> None:
      df.drop(df[~mask].index, inplace=True)
  ```
- **Config dicts (`workflow_args`) are read-only inside pure functions.** Parse into a Pydantic model for reads. If you need to mutate (e.g., add a computed timestamp), do so at the orchestration layer, not in pure logic.

### Controlled I/O side effects
When a function must perform I/O (read/write files, call an external service), make the side effect the function's explicit purpose — not a hidden consequence:
- File mutation functions (read parquet → filter → write back) are fine, but they belong in their own module (`filters.py`), not mixed with pure computation.
- I/O functions are named to signal the side effect: `enrich_*`, `merge_*`, `sync_*`.

### Pydantic models are immutable by default
- Pydantic `BaseModel` fields are not reassigned after construction. Models are data containers, not mutable state machines.
- Logic on models is limited to derived-property methods (e.g., `is_incremental_ready()` that reads existing fields).

### Temporary state mutation
If a method must temporarily mutate instance state (e.g., swapping a SQL template), always restore in a `finally` block:
```python
saved = self.query_sql
self.query_sql = modified_sql
try:
    return await super().execute(...)
finally:
    self.query_sql = saved
```

---

## 8. Logging & Observability

### Logger setup
- **One logger per module, created at module level:**
  ```python
  from <project>.observability import get_logger
  logger = get_logger(__name__)
  ```
  Never create loggers inside functions or class methods.
- **Centralise the logger factory.** All modules import `get_logger` from a single internal `observability` module — not directly from `logging` or third-party SDK modules. This gives one place to change log format, level, or destination.

### Log level semantics
| Level | Use for | Example |
|-------|---------|---------|
| `debug` | Diagnostic detail useful during development | Record counts, filter decisions, intermediate values |
| `info` | Lifecycle events visible in production | Operation started, operation completed with summary, record counts |
| `warning` | Degraded but recoverable scenarios | Optional feature unavailable, fallback path taken |
| `error` | Caught exception with context, before re-raising | Connection failed, query failed, file not found |

### Log message quality
- **Every log message must carry context.** Include relevant variable values inline:
  ```python
  # Good
  logger.info(f"Loaded {len(records)} records from {table_name}")
  # Bad
  logger.info("Done")
  ```
- **Use structured `extra` dicts** when the logging framework supports it:
  ```python
  logger.info("Fetching schemas", extra={"database": db_name, "strategy": "AU"})
  ```
- **Never use `print()` in production code.**

### Observability architecture
- **All observability primitives (logger, metrics, traces) are re-exported from a single internal module** (e.g., `app/observability/telemetry.py`). This module contains no logic — it is a thin re-export hub:
  ```python
  from sdk.observability.logger import get_logger
  from sdk.observability.metrics import get_metrics, MetricType
  from sdk.observability.traces import get_traces
  __all__ = ["get_logger", "get_metrics", "get_traces", "MetricType"]
  ```
  Application code never imports observability directly from the SDK or from `logging`.
- **Observability is orthogonal to business logic.** Logging statements are additive — removing every `logger.*` call from a function must not change its behaviour. No observability object is passed as a parameter to business logic functions, and no return value depends on telemetry.
- **Metrics follow a consistent labelling convention.** Gauges, counters, and histograms include labels that identify the component (`workflow_type`, `activity_name`, `status`). Define metric names and label sets in the config or observability module, not scattered across business logic.

### Error logging strategy
- **Warnings for recoverable errors** — an optional view is unavailable, a single record failed enrichment but the batch continues.
- **Errors for fatal failures** — the operation cannot continue and the exception will propagate.
- **Debug for diagnostic info** — result counts, filter decisions, intermediate state. Not shown in production logs by default.

---

## 9. Error Handling

- **Log then re-raise** for caught exceptions where the caller needs to know:
  ```python
  except Exception as e:
      logger.error(f"Failed to do X: {e}")
      raise
  ```
- **Define a custom exception hierarchy** for each module that owns a domain boundary. Place it in `errors.py` with a base class and specialised subclasses:
  ```python
  class StorageError(Exception): ...
  class StorageNotFoundError(StorageError): ...
  ```
  Callers catch the specific subclass; broad code catches the base.
- **Graceful fallback paths** (expected alternative scenarios) use `logger.warning()` and continue — they do not raise exceptions.
- **Do not duplicate validation** that Pydantic already handles. If the model raises `ValidationError`, let it propagate.
- **Avoid bare `except:` or `except Exception:` without re-raising** unless the intent is to suppress and that choice is explicitly documented with a comment.
- **Tolerant iteration** — when processing a batch of items and one fails, log a warning per failure and continue. Do not let one bad record abort the entire batch unless the failure is systemic.

---

## 10. Type Annotations

- **All function signatures are fully annotated** — parameters and return type, no exceptions.
- **Use `Optional[T]` for nullable types** (consistent with the rest of the codebase; do not mix with `T | None` union style).
- **Use subscripted generics** from `typing`: `Dict[str, Any]`, `List[str]`, `Tuple[str, ...]`, `Callable[..., Any]`.
- **`# type: ignore` is a last resort**, used only on a single line where no other solution exists. Never silence a whole block.
- **Do not widen types to `Any` to silence pyright.** Fix the underlying type issue instead.
- **Pyright standard mode must pass** with no new errors after every change.

---

## 11. Configuration & Pydantic Models

- **All external configuration is parsed through a Pydantic model** at the entry point where it is first used. Never read raw dict keys throughout the codebase:
  ```python
  # Good
  config = MyConfig.model_validate(raw_dict)
  value = config.some_field
  # Bad
  value = raw_dict.get("some-field", "default")
  ```
- **Models use `Field(default=..., alias="...")` for keys that differ from Python naming conventions** (e.g., hyphenated keys from config files or environment variables).
- **Models use `ConfigDict(populate_by_name=True)`** so both aliased and Python-style keys are accepted.
- **Feature toggles stored as `str` (`"true"`/`"false"`)** if the external config source sends strings. Do not silently coerce to `bool` in the model — keep the type honest.
- **New config keys get a model field with a safe default.** No ad-hoc `.get()` calls scattered across the codebase.
- **Mutations to the raw config dict** (e.g., adding computed fields) go directly to the raw dict, not via `model_dump()`. Framework/SDK calls always receive the raw dict.

---

## 12. Import Organisation

- **Enforce import order** with isort (black profile):
  1. `from __future__ import annotations`
  2. Standard library
  3. Third-party libraries
  4. Local application imports
- **Within a package, use relative imports** (`.models`, `.utils`). Across packages, use absolute imports (`from app.extract import ...`).
- **Wildcard imports (`from x import *`) only in `__init__.py` re-export files.** Never in implementation files.
- **No unused imports.** They are a lint error (ruff F401) and signal dead code.

---

## 13. Constants & Configuration Values

- **Module-level constants are UPPER_SNAKE_CASE** and defined at the top of the file, below imports.
- **Magic values do not appear inline in logic.** Extract them to a constant with a name that explains their meaning.
- **Environment variables are read with a default:**
  ```python
  DEBUG = os.getenv("DEBUG", "false").lower() == "true"
  ```
  Never call `os.environ["KEY"]` without a fallback unless the variable is truly required and its absence should crash at startup.
- **Timeout and threshold values live in a dedicated `config.py`** module, not scattered as literals in workflow or handler files.

---

## 14. Facade & Abstraction Patterns

- **Wrap third-party or SDK interfaces behind an internal facade** when the interface is used in many places. This keeps SDK-specific details out of business logic and makes future replacements cheap.
- **Use a factory function or factory dict** to select concrete implementations at runtime based on configuration (e.g., storage provider from YAML). Do not hard-code `if/elif` chains for backend selection — use a dispatch dict:
  ```python
  builders = {
      "s3": lambda: S3Store(config.bucket, **config.options),
      "gcs": lambda: GCSStore(config.bucket, **config.options),
      "local": lambda: LocalStore(config.bucket),
  }
  store = builders[config.provider]()
  ```
- **Static method facades** are appropriate when the abstraction has no instance state and the interface should feel like a utility. Use `@classmethod` with a class-level cache for lazy initialisation:
  ```python
  class ObjectStore:
      _stores: Dict[str, Backend] = {}

      @classmethod
      def _get_store(cls, name: str) -> Backend:
          if name not in cls._stores:
              cls._stores[name] = create_store(name)
          return cls._stores[name]

      @classmethod
      async def download(cls, source, dest, store_name): ...
  ```
- **The facade is the only import point for its abstraction.** Application code never imports the underlying library directly.

---

## 15. Thin Orchestration Wrappers

- **Orchestration layer methods (activities, route handlers) are thin.** They handle framework plumbing (deserialising input, setting up context) and immediately delegate to a pure function in the logic layer.
- **No business logic lives in orchestration methods.** If you find yourself writing more than a few lines of data transformation inside an activity or handler, extract it to a function in the logic layer and call it.
- **Pure logic functions have no framework imports.** A function in `utils.py` or `filters.py` must be callable from a plain Python script or a unit test with no framework dependencies.

---

## 16. Async Patterns

- **Use `asyncio.Semaphore` to bound concurrency** when fanning out to an external service. Do not fire unlimited `gather()` calls:
  ```python
  sem = asyncio.Semaphore(10)
  async def bounded_fetch(item):
      async with sem:
          return await client.fetch(item)
  results = await asyncio.gather(*[bounded_fetch(i) for i in items])
  ```
- **Separate construction from async initialisation.** Constructors (`__init__`) are sync and cheap. Expensive async setup (connections, credential resolution) happens in a dedicated `async load()` or `async setup()` method.
- **Async functions belong in the handler and orchestration layers.** Pure computation in the logic layer should remain synchronous unless it genuinely performs I/O (e.g., calling an external service via a Protocol).

---

## 17. Testing

- **Test files mirror the module they test.** `app/extract/filters.py` → `tests/unit/test_filters.py`. This makes it trivial to find tests for any module.
- **Shared fixtures live in `conftest.py`.** Do not duplicate fixture setup across test files.
- **Mock at the import location of the caller**, not the definition location:
  ```python
  # Good — patches what the module under test actually imports
  patch("app.handler.handler.read_sql_files")
  # Bad — patches the original definition location
  patch("sdk.utils.read_sql_files")
  ```
- **Use `AsyncMock` for async methods** and `MagicMock` for sync methods. Do not mix them.
- **Async tests require `@pytest.mark.asyncio`.**
- **Tests must not make real network calls.** Every external dependency (database, API, storage) is mocked.
- **Maintain the project's minimum coverage threshold.** New modules must include tests sufficient to keep the project above it.

---

## 18. Comments & Documentation

- **Module-level docstrings are required** for every non-trivial `.py` file. Cover: what it does, what it exports, and any notable constraints or dependencies.
- **Public functions and methods need docstrings** that describe: purpose, important parameters, return value, and any side effects or preconditions.
- **Inline comments explain *why*, not *what*.** The code says what; comments explain the intent or constraint behind a non-obvious choice:
  ```python
  # Good: # Strip suffix if users paste the full hostname instead of the account ID
  # Bad:  # Call strip()
  ```
- **Do not leave commented-out code** in committed files. Use version control history instead.
- **Non-Python asset files (SQL, YAML, config) include a header comment** stating the file's purpose and any placeholders or variables it expects.

---

## 19. Pre-commit & Code Quality Gates

- **Run the full lint + format pipeline before every commit:**
  ```bash
  ruff check --fix . && ruff format . && isort --profile black .
  ```
- **Commit messages follow Conventional Commits:** `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`.
- **No debug statements** (`breakpoint()`, `pdb`, `ipdb`) in committed code.
- **No trailing whitespace** on any line.
- **All hooks must pass.** Do not use `--no-verify` to skip them. Fix the root cause instead.
