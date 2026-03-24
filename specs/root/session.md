# session.py

## Summary

Central HTTP session management module for the gs_quant SDK. Provides a singleton-pattern session (via `ContextBase` with thread-local storage) that handles authentication (OAuth2, Kerberos/GSSSO, pass-through bearer tokens, MarqueeLogin), synchronous and asynchronous HTTP requests (GET/POST/PUT/DELETE), WebSocket connections, response parsing (JSON and msgpack), automatic 401 retry with re-authentication, and domain/environment resolution from `config.ini`. In an Elixir port this maps directly to a GenServer holding connection state, authentication tokens, and HTTP client references.

## Dependencies

- Internal:
  - `gs_quant` (`version` as `APP_VERSION`)
  - `gs_quant.base` (`Base`)
  - `gs_quant.context_base` (`ContextBase`, `nullcontext`)
  - `gs_quant.errors` (`MqError`, `MqRequestError`, `MqAuthenticationError`, `MqUninitialisedError`, `error_builder`)
  - `gs_quant.json_encoder` (`JSONEncoder`, `encode_default`)
  - `gs_quant.tracing` (`Tracer`, `TracingScope`, `Tags`)
  - `gs_quant_auth.kerberos.session_kerberos` (`KerberosSessionMixin`, `MQLoginMixin`) -- optional, guarded by `try/except ModuleNotFoundError`
- External:
  - `asyncio`
  - `inspect`
  - `itertools`
  - `json`
  - `logging`
  - `os`
  - `ssl`
  - `sys`
  - `abc` (`abstractmethod`)
  - `configparser` (`ConfigParser`)
  - `contextlib` (`asynccontextmanager`)
  - `enum` (`Enum`, `auto`, `unique`)
  - `typing` (`Optional`, `Union`, `Iterable`, `Any`)
  - `backoff` (`on_exception`, `on_predicate`)
  - `certifi` (`where`)
  - `httpx` (`AsyncClient`, `HTTPTransport`)
  - `msgpack` (`dumps`, `unpackb`)
  - `pandas` (`pd.DataFrame`)
  - `requests` (`Session`, `adapters.HTTPAdapter`, `cookies.create_cookie`, `exceptions.HTTPError`, `exceptions.Timeout`)
  - `urllib3` (`poolmanager.PoolManager`, `disable_warnings`, `exceptions.InsecureRequestWarning`)
  - `websockets` (`connect`, `__version__`) -- lazy-imported inside `_connect_websocket_raw`

## Type Definitions

### Environment (Enum, @unique)
See **Enums and Constants** section.

### Domain (plain class, used as string constants)
See **Enums and Constants** section.

### CustomHttpAdapter (class)
Inherits: `requests.adapters.HTTPAdapter`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__ssl_ctx` (class-level) | `Optional[ssl.SSLContext]` | `None` | Lazily-initialized singleton SSL context shared across all adapter instances |

### _SyncSessionAPI (class)
Wraps a `GsSession` to expose synchronous `get`, `post`, `put`, `delete` methods. Thin delegation layer.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `_session` | `GsSession` | (required) | The parent session to delegate calls to |

### _AsyncSessionAPI (class)
Wraps a `GsSession` to expose async `get`, `post`, `put`, `delete`, `connect_websocket` methods. Thin delegation layer.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `_session` | `GsSession` | (required) | The parent session to delegate calls to |

### GsSession (class, abstract)
Inherits: `ContextBase` (which uses `ContextMeta` metaclass for thread-local singleton via `current` class property)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__config` (class-level) | `Optional[ConfigParser]` | `None` | Lazily-loaded INI config; shared across all instances |
| `_session` | `Optional[requests.Session]` | `None` | Synchronous HTTP session; created in `init()` |
| `_session_async` | `Optional[httpx.AsyncClient]` | `None` | Asynchronous HTTP client; created in `_init_async()` |
| `_sync_api` | `Optional[_SyncSessionAPI]` | `None` | Lazily-created sync API facade |
| `_async_api` | `Optional[_AsyncSessionAPI]` | `None` | Lazily-created async API facade |
| `domain` | `str` | (required) | Resolved base URL for API calls |
| `_orig_domain` | `str` | same as `domain` | Original domain key (e.g., `Domain.APP`) before URL resolution; used to decide post-init activity service call |
| `environment` | `Environment` | `Environment.DEV` | Resolved environment enum |
| `api_version` | `str` | `API_VERSION` (`"v1"`) | API version path segment |
| `application` | `str` | `DEFAULT_APPLICATION` (`"gs-quant"`) | Application name sent in `X-Application` header |
| `verify` | `bool` | `True` | SSL verification flag |
| `http_adapter` | `requests.adapters.HTTPAdapter` | see logic | HTTP adapter; auto-selected based on OpenSSL version if not provided |
| `application_version` | `str` | `APP_VERSION` | Version sent in `X-Version` header |
| `proxies` | `Optional[Any]` | `None` | Proxy configuration for `requests.Session` |
| `mounts` | `Optional[dict]` | `None` | `httpx` transport mounts derived from `proxies` |
| `redirect_to_mds` | `bool` | `False` | Whether to redirect to MDS domain |
| `__close_on_exit` | `bool` | (set at enter) | Whether to tear down `_session` on context exit |
| `__close_on_exit_async` | `bool` | (set at aenter) | Whether to tear down `_session_async` on async context exit |

#### GsSession.Scopes (nested Enum)
See **Enums and Constants** section.

### OAuth2Session (class)
Inherits: `GsSession`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auth_url` | `str` | from config | OAuth2 token endpoint URL |
| `client_id` | `str` | (required) | OAuth2 client ID |
| `client_secret` | `str` | (required) | OAuth2 client secret |
| `scopes` | `tuple[str, ...]` | (required) | OAuth2 scopes to request |
| `_orig_domain` | `str` | domain key | Overwritten after super().__init__ |
| `verify` | `bool` | may be set to `False` | Overridden to `False` for DEV env or non-AppDomain URLs (except MDS_US_EAST) |

### PassThroughSession (class)
Inherits: `GsSession`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `__config` (class-level) | `Optional[ConfigParser]` | `None` | Own lazily-loaded config (separate from GsSession's) |
| `token` | `str` | (required) | Pre-existing bearer token |
| `_orig_domain` | `str` | resolved domain | Domain after config resolution |

### KerberosSession (class, conditional)
Inherits: `KerberosSessionMixin`, `GsSession` (MRO: KerberosSessionMixin first)
Only defined if `gs_quant_auth.kerberos.session_kerberos.KerberosSessionMixin` is importable.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| (all GsSession fields) | | | Inherited; domain and verify resolved via `KerberosSessionMixin.domain_and_verify()` |

### PassThroughGSSSOSession (class, conditional)
Inherits: `KerberosSessionMixin`, `GsSession`
Only defined if `gs_quant_auth` is importable.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `token` | `str` | (required) | GSSSO cookie token |
| `csrf_token` | `Optional[str]` | `None` | CSRF token for Marquee |

### MQLoginSession (class, conditional)
Inherits: `MQLoginMixin`, `GsSession`
Only defined if `gs_quant_auth.kerberos.session_kerberos.MQLoginMixin` is importable.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mq_login_token` | `Optional[str]` | `None` | MarqueeLogin token |
| `_orig_domain` | `str` | domain key | Overwritten after super().__init__ |

## Enums and Constants

### Environment (Enum, @unique)
| Value | Raw | Description |
|-------|-----|-------------|
| `DEV` | `auto()` (1) | Development environment |
| `QA` | `auto()` (2) | QA environment |
| `PROD` | `auto()` (3) | Production environment |

### GsSession.Scopes (nested Enum)
| Value | Raw | Description |
|-------|-----|-------------|
| `READ_CONTENT` | `"read_content"` | Read content scope |
| `READ_FINANCIAL_DATA` | `"read_financial_data"` | Read financial data scope |
| `READ_PRODUCT_DATA` | `"read_product_data"` | Read product data scope |
| `READ_USER_PROFILE` | `"read_user_profile"` | Read user profile scope |
| `MODIFY_CONTENT` | `"modify_content"` | Modify content scope |
| `MODIFY_FINANCIAL_DATA` | `"modify_financial_data"` | Modify financial data scope |
| `MODIFY_PRODUCT_DATA` | `"modify_product_data"` | Modify product data scope |
| `MODIFY_USER_PROFILE` | `"modify_user_profile"` | Modify user profile scope |
| `RUN_ANALYTICS` | `"run_analytics"` | Run analytics scope |
| `EXECUTE_TRADES` | `"execute_trades"` | Execute trades scope |

### Domain (plain class with string constants)
| Name | Value | Description |
|------|-------|-------------|
| `MDS_US_EAST` | `"MdsDomainEast"` | Market Data Services US East key (resolves via config.ini) |
| `MDS_WEB` | `"MdsWebDomain"` | Market Data Services Web key |
| `APP` | `"AppDomain"` | Default application API domain key |

### Module Constants
| Name | Type | Value | Description |
|------|------|-------|-------------|
| `API_VERSION` | `str` | `"v1"` | Default API version path segment |
| `DEFAULT_APPLICATION` | `str` | `"gs-quant"` | Default application name header value |
| `DEFAULT_TIMEOUT` | `int` | `65` | Default HTTP request timeout in seconds |
| `logger` | `logging.Logger` | `logging.getLogger(__name__)` | Module-level logger |

## Functions/Methods

### CustomHttpAdapter.ssl_context() -> ssl.SSLContext (classmethod)
Purpose: Return a lazily-initialized singleton SSL context with legacy compatibility settings.

**Algorithm:**
1. Branch: `cls.__ssl_ctx is None` -> create new `ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)`
   - Set `check_hostname = False`
   - Set `verify_mode = 0`
   - Set `options |= 0x4` (OP_LEGACY_SERVER_CONNECT)
   - Call `load_default_certs()`
   - Call `load_verify_locations(certifi.where())`
2. Branch: `cls.__ssl_ctx is not None` -> return cached instance
3. Return `cls.__ssl_ctx`

### CustomHttpAdapter.init_poolmanager(self, connections: int, maxsize: int = 100, block: bool = False) -> None
Purpose: Override pool manager initialization to inject custom SSL context.

**Algorithm:**
1. Create `urllib3.poolmanager.PoolManager` with `num_pools=connections`, `maxsize=maxsize`, `block=block`, `ssl_context=self.ssl_context()`
2. Assign to `self.poolmanager`

### GsSession.Scopes.get_default() -> tuple[str, ...] (classmethod)
Purpose: Return default OAuth2 scope values.

**Algorithm:**
1. Return tuple of `(READ_CONTENT.value, READ_PRODUCT_DATA.value, READ_FINANCIAL_DATA.value, READ_USER_PROFILE.value)`

### GsSession.__init__(self, domain: str, environment: str = None, api_version: str = API_VERSION, application: str = DEFAULT_APPLICATION, verify=True, http_adapter: requests.adapters.HTTPAdapter = None, application_version=APP_VERSION, proxies=None, redirect_to_mds=False) -> None
Purpose: Initialize session state, resolve environment enum, configure HTTP adapter.

**Algorithm:**
1. Call `super().__init__()` (ContextBase)
2. Set `_session = None`, `_session_async = None`, `_sync_api = None`, `_async_api = None`
3. Set `self.domain = domain`, `self._orig_domain = domain`
4. Resolve `self.environment`:
   - Branch: `environment in tuple(x.name for x in Environment)` -> `Environment[environment]`
   - Branch: `isinstance(domain, Environment)` -> `self.environment = domain`
   - Branch: else -> `Environment.DEV`
5. Set `api_version`, `application`, `verify`
6. Resolve `http_adapter`:
   - Branch: `http_adapter is None` AND `ssl.OPENSSL_VERSION_INFO >= (3, 0, 0)` -> `CustomHttpAdapter()`
   - Branch: `http_adapter is None` AND OpenSSL < 3.0 -> `requests.adapters.HTTPAdapter(pool_maxsize=100)`
   - Branch: `http_adapter is not None` -> use provided adapter
7. Set `application_version`, `proxies`
8. Build `mounts`:
   - Branch: `proxies` is truthy -> `{key: httpx.HTTPTransport(proxy=val) for key, val in proxies}`
   - Branch: `proxies` is falsy -> `None`
9. Set `redirect_to_mds`

### GsSession._authenticate(self) -> None (abstract, decorated with backoff)
Purpose: Abstract method; subclasses implement actual authentication.

**Decorators:**
- `@backoff.on_exception(lambda: backoff.expo(factor=2), (requests.exceptions.HTTPError, requests.exceptions.Timeout), max_tries=5)`
- `@backoff.on_predicate(lambda: backoff.expo(factor=2), lambda x: x.status_code in (500, 502, 503, 504), max_tries=5)`
- `@abstractmethod`

**Algorithm:**
1. Raise `NotImplementedError("Must implement _authenticate")`

### GsSession._authenticate_async(self) -> None
Purpose: Copy sync session auth headers and cookies to the async httpx client.

**Algorithm:**
1. Branch: `self._has_async_session()` is `True`:
   - Copy all headers from `self._session.headers` to `self._session_async.headers`
   - Iterate `self._session.cookies`, set each on `self._session_async.cookies` (name, value, domain)
2. Branch: async session not open -> do nothing

### GsSession._authenticate_all_sessions(self) -> None
Purpose: Re-authenticate sync session then propagate to async session.

**Algorithm:**
1. Call `self._authenticate()`
2. Call `self._authenticate_async()`

### GsSession._on_enter(self) -> None
Purpose: Context manager enter hook; ensure session is initialized.

**Algorithm:**
1. Set `self.__close_on_exit = (self._session is None)`
2. Branch: `not self._session` -> call `self.init()`

### GsSession._on_exit(self, exc_type, exc_val, exc_tb) -> None
Purpose: Context manager exit hook; clean up if session was created on enter.

**Algorithm:**
1. Branch: `self.__close_on_exit` -> set `self._session = None`, `self._session_async = None`

### GsSession._has_async_session(self) -> bool
Purpose: Check if async client exists and is not closed.

**Algorithm:**
1. Return `self._session_async and not self._session_async.is_closed`

### GsSession._init_async(self) -> None
Purpose: Lazily initialize the httpx async client.

**Algorithm:**
1. Branch: `not self._has_async_session()`:
   - Create `httpx.AsyncClient(follow_redirects=True, verify=CustomHttpAdapter.ssl_context(), mounts=self.mounts)`
   - Set `X-Application` and `X-Version` headers
   - Call `self._authenticate_async()`
2. Branch: already has async session -> do nothing

### GsSession._on_aenter(self) -> None (async)
Purpose: Async context manager enter; initialize both sync and async sessions.

**Algorithm:**
1. Set `self.__close_on_exit = (self._session is None)`
2. Set `self.__close_on_exit_async = (not self._has_async_session())`
3. Branch: `self.__close_on_exit` -> call `self.init()`
4. Branch: `self.__close_on_exit_async` -> call `self._init_async()`

### GsSession._on_aexit(self, exc_type, exc_val, exc_tb) -> None (async)
Purpose: Async context manager exit; tear down sessions created on aenter.

**Algorithm:**
1. Branch: `self.__close_on_exit_async`:
   - Branch: `self._has_async_session()` -> `await self._session_async.aclose()`
   - Set `self._session_async = None`
2. Branch: `self.__close_on_exit` -> set `self._session = None`

### GsSession.init(self) -> None
Purpose: Initialize the synchronous requests.Session, mount adapter, authenticate, optionally post to activity service.

**Algorithm:**
1. Branch: `not self._session`:
   - Create `requests.Session()`
   - Branch: `self.http_adapter is not None` -> mount on `https://`
   - Branch: `self.proxies is not None` -> set `self._session.proxies`
   - Set `self._session.verify = self.verify`
   - Update headers: `X-Application`, `X-Version`
   - Call `self._authenticate()`
   - Branch: `self._orig_domain == Domain.APP` -> call `self.post_to_activity_service()`
2. Branch: `self._session` already set -> do nothing

### GsSession.close(self) -> None
Purpose: Close sync and async sessions, release resources.

**Algorithm:**
1. Branch: `self._session` is truthy:
   - Branch: `self.http_adapter is None` -> call `self._session.close()` (don't close shared adapter)
   - Set `self._session = None`
2. Branch: `self._session_async` is truthy:
   - Try `asyncio.run(self._close_async())`
   - Catch all exceptions -> pass (swallow)

### GsSession._close_async(self) -> None (async)
Purpose: Close the httpx async client.

**Algorithm:**
1. Branch: `self._session_async` -> `await self._session_async.aclose()`, set to `None`

### GsSession.__del__(self) -> None
Purpose: Destructor; calls `self.close()`.

### GsSession.__unpack(results: Union[dict, list], cls: type) -> Union[Base, tuple, dict] (staticmethod)
Purpose: Deserialize API results into typed objects.

**Algorithm:**
1. Branch: `issubclass(cls, Base)`:
   - Branch: `isinstance(results, list)` -> return `tuple(None if r is None else cls.from_dict(r) for r in results)`
   - Branch: dict -> return `None if results is None else cls.from_dict(results)`
2. Branch: not subclass of Base:
   - Branch: `isinstance(results, list)` -> return `tuple(cls(**r) for r in results)`
   - Branch: dict -> return `cls(**results)`

### GsSession._build_url(self, domain: Optional[str], path: str, include_version: Optional[bool]) -> str
Purpose: Build the full request URL from domain, version, and path.

**Algorithm:**
1. Branch: `not domain` -> use `self.domain`
2. Return `f"{domain}/{self.api_version if include_version else ''}{path}"` (format: `{domain}[/v1]{path}`)

### GsSession._build_request_params(self, method: str, path: str, url: str, payload: Optional[Union[dict, str, bytes, Base, pd.DataFrame]], request_headers: Optional[dict], timeout: Optional[int], use_body: bool, data_key: str, tracing_scope: Optional[TracingScope]) -> dict
Purpose: Build the kwargs dict for `requests.Session.request()` or `httpx.AsyncClient.request()`.

**Algorithm:**
1. Check `is_dataframe = isinstance(payload, pd.DataFrame)`
2. Branch: `not is_dataframe` -> set `payload = payload or {}`
3. Initialize `kwargs = {'timeout': timeout}`
4. Branch: `tracing_scope` -> set tags: `path`, `timeout`, `HTTP_URL`, `HTTP_METHOD`, `span.kind=client`
5. Branch: method in `('GET', 'DELETE')` AND `not use_body`:
   - Set `kwargs['params'] = payload`
   - Branch: `tracing_scope or request_headers`:
     - Copy session headers, merge `request_headers`
     - Branch: `tracing_scope` -> inject tracing headers
     - Set `kwargs['headers']`
6. Branch: method in `('POST', 'PUT')` OR `(method in ('GET', 'DELETE') and use_body)`:
   - Copy session headers, merge `request_headers`
   - Branch: `tracing_scope` -> inject tracing headers
   - Branch: `'Content-Type' not in headers` -> set `'application/json; charset=utf-8'`
   - Branch: `tracing_scope` -> tag `request.content.type`
   - Check `use_msgpack = headers.get('Content-Type') == 'application/x-msgpack'`
   - Branch: `use_msgpack` -> set `Accept` header to Content-Type
   - Set `kwargs['headers']`
   - Branch: `is_dataframe or payload`:
     - Branch: `isinstance(payload, (str, bytes))` -> use raw payload
     - Branch: `use_msgpack` -> `msgpack.dumps(payload, default=encode_default)`
     - Branch: else -> `json.dumps(payload, cls=JSONEncoder)`
     - Set `kwargs[data_key]` (either `"data"` for sync or `"content"` for async)
7. Branch: else (unrecognized method combination) -> raise `MqError('not implemented')`
8. Return `kwargs`

### GsSession._parse_response(self, request_id, response, method: str, url: str, cls: Optional[type], return_request_id: Optional[bool]) -> Union[Base, tuple, dict]
Purpose: Parse HTTP response, deserialize body, optionally unpack into typed objects.

**Algorithm:**
1. Branch: `not 199 < response.status_code < 300`:
   - Determine `reason`: use `response.reason` if present, else `response.reason_phrase` (httpx)
   - Branch: Content-Type is `'text/html'` -> error message is just `reason`
   - Branch: else -> error message is `f'{reason}: {response.text}'`
   - Raise `error_builder(response.status_code, err_msg, context=f'{request_id}: {method} {url}')`
2. Branch: `'Content-Type' in response.headers`:
   - Branch: `'application/x-msgpack'` in Content-Type -> `msgpack.unpackb(response.content, raw=False)`
   - Branch: `'application/json'` in Content-Type -> `json.loads(response.text)`
   - Branch: else -> `{'raw': response}`
   - Branch: `cls and ret`:
     - Branch: `isinstance(ret, dict) and 'results' in ret` -> unpack `ret['results']`
     - Branch: else -> unpack entire `ret`
   - Branch: `return_request_id` -> return `(ret, request_id)`, else return `ret`
3. Branch: no Content-Type header:
   - `ret = {'raw': response}`
   - Branch: `return_request_id` -> add `ret['request_id'] = request_id`
   - Return `ret`

### GsSession.__request(self, method: str, path: str, payload=None, request_headers=None, cls=None, try_auth=True, include_version=True, timeout=DEFAULT_TIMEOUT, return_request_id=False, use_body=False, domain=None) -> Union[Base, tuple, dict]
Purpose: Execute a synchronous HTTP request with tracing, 401 retry logic.

**Algorithm:**
1. Get active tracing span
2. Build URL via `_build_url()`
3. Branch: span exists and recording -> create `Tracer(url)` context; else `nullcontext()`
4. Within tracing context:
   - Build request params via `_build_request_params()` with `data_key="data"`
   - Execute `self._session.request(method, url, **kwargs)`
   - Extract `x-dash-requestid` header
   - Branch: scope exists -> set tracing tags (status_code, error if >399, request_id, content-type)
5. Branch: `response.status_code == 401`:
   - Branch: `not try_auth` -> raise `MqRequestError`
   - Call `self._authenticate()`
   - Recursive call to `self.__request()` with `try_auth=False`
6. Return `self._parse_response(...)`

### GsSession.__request_async(self, method: str, path: str, payload=None, request_headers=None, cls=None, try_auth=True, include_version=True, timeout=DEFAULT_TIMEOUT, return_request_id=False, use_body=False, domain=None) -> Union[Base, tuple, dict] (async)
Purpose: Execute an asynchronous HTTP request with tracing, 401 retry logic.

**Algorithm:**
1. Call `self._init_async()` to ensure async client exists
2. Get active tracing span
3. Build URL via `_build_url()`
4. Branch: span exists and recording -> create `Tracer(f'http:/{path}')` context (note: different from sync's `Tracer(url)`)
5. Within tracing context:
   - Build request params via `_build_request_params()` with `data_key="content"`
   - Execute `await self._session_async.request(method, url, **kwargs)`
   - Extract `x-dash-requestid` header
   - Branch: scope exists -> set tracing tags (status_code, request_id)
6. Branch: `response.status_code == 401`:
   - Log debug message
   - Branch: `not try_auth` -> raise `MqRequestError`
   - Call `self._authenticate_all_sessions()` (re-auths both sync and async)
   - Await recursive call with `try_auth=False`
7. Return `self._parse_response(...)`

### GsSession._get(self, path, payload=None, request_headers=None, cls=None, include_version=True, timeout=DEFAULT_TIMEOUT, return_request_id=False, domain=None) -> Union[Base, tuple, dict]
Purpose: Synchronous GET. Delegates to `__request('GET', ...)`.

### GsSession._get_async(self, ...) -> Union[Base, tuple, dict] (async)
Purpose: Async GET. Delegates to `__request_async('GET', ...)`.

### GsSession._post(self, path, payload=None, request_headers=None, cls=None, include_version=True, timeout=DEFAULT_TIMEOUT, return_request_id=False, domain=None) -> Union[Base, tuple, dict]
Purpose: Synchronous POST. Delegates to `__request('POST', ...)`.

### GsSession._post_async(self, ...) -> Union[Base, tuple, dict] (async)
Purpose: Async POST. Delegates to `__request_async('POST', ...)`.

### GsSession._put(self, path, payload=None, request_headers=None, cls=None, include_version=True, timeout=DEFAULT_TIMEOUT, return_request_id=False, domain=None) -> Union[Base, tuple, dict]
Purpose: Synchronous PUT. Delegates to `__request('PUT', ...)`.

### GsSession._put_async(self, ...) -> Union[Base, tuple, dict] (async)
Purpose: Async PUT. Delegates to `__request_async('PUT', ...)`.

### GsSession._delete(self, path, payload=None, request_headers=None, cls=None, include_version=True, timeout=DEFAULT_TIMEOUT, return_request_id=False, use_body=False, domain=None) -> Union[Base, tuple, dict]
Purpose: Synchronous DELETE. Delegates to `__request('DELETE', ...)`. Note: `use_body` param allows sending payload in body rather than query params.

### GsSession._delete_async(self, ..., use_body=False, ...) -> Union[Base, tuple, dict] (async)
Purpose: Async DELETE. Delegates to `__request_async('DELETE', ...)`.

### GsSession._connect_websocket(self, path: str, headers: Optional[dict] = None, include_version=True, domain: Optional[str] = None, **kwargs: Any) -> AsyncContextManager (async context manager)
Purpose: Open a WebSocket connection with tracing support.

**Algorithm:**
1. Get active tracing span
2. Branch: span exists and recording -> create `Tracer(f'wss:/{path}')` context; else `nullcontext()`
3. Inject tracing headers
4. Merge `headers` with tracing headers (tracing headers override)
5. Delegate to `self._connect_websocket_raw()`
6. Branch: `scope and scope.span`:
   - Branch: `hasattr(websocket, 'request_headers')` (websockets < 14) -> tag `wss.host` from `request_headers`
   - Branch: else (websockets >= 14) -> tag `wss.host` from `request.headers`
7. Yield websocket

### GsSession._connect_websocket_raw(self, path: str, headers: Optional[dict] = None, include_version=True, domain: Optional[str] = None, **kwargs: Any) -> websockets connection context
Purpose: Build WebSocket URL and call `websockets.connect()`.

**Algorithm:**
1. Lazy-import `websockets`
2. Parse `_WEBSOCKETS_VERSION` from `websockets.__version__`
3. Build URL:
   - Branch: `domain` provided -> `f'{domain}{version_path}{path}'`
   - Branch: no domain -> `f'ws{self.domain[4:]}{version_path}{path}'` (replaces `http` prefix with `ws`)
4. Build `extra_headers` from `self._headers()` + merged `headers`
5. Branch: `_WEBSOCKETS_VERSION >= (14, 0)` -> use `additional_headers` kwarg
6. Branch: else -> use `extra_headers` kwarg with `read_limit=2**32`
7. Return `websockets.connect(url, max_size=2**32, ssl=CustomHttpAdapter.ssl_context() if wss else None, ...)`

### GsSession._headers(self) -> list[tuple[str, str]]
Purpose: Extract authentication-related headers and cookies for WebSocket connections.

**Algorithm:**
1. Initialize empty `headers` list
2. Branch: `self._session` exists:
   - Iterate session headers; include only `AUTHORIZATION`, `X-MARQUEE-CSRF-TOKEN`, `X-APPLICATION`, `X-VERSION` (case-insensitive upper check)
   - Initialize empty `cookies` tuple
   - Branch: `self._session.cookies` exists:
     - Branch: `"MarqueeLogin"` in cookies -> append `MarqueeLogin=...`
     - Branch: `"MARQUEE-CSRF-TOKEN"` in cookies -> append `MARQUEE-CSRF-TOKEN=...`
     - Branch: `"GSSSO"` in cookies -> append `GSSSO=...`
   - Branch: `cookies` non-empty -> append `('Cookie', "; ".join(cookies))`
3. Return headers

### GsSession._get_mds_domain(self) -> str
Purpose: Resolve the MDS domain for the current environment.

**Algorithm:**
1. Load environment config via `_config_for_environment(self.environment.name)`
2. Strip `.web` from current domain: `self.domain.replace('marquee.web', 'marquee')`
3. Branch: environment is QA -> also strip `.web` from QA domain
4. Evaluate three booleans: `is_mds_web`, `is_env_mds_web`, `is_env_marquee_web`
5. Branch: any of the three is true -> return `env_config['MdsWebDomain']`
6. Branch: none true -> return `env_config['MdsDomainEast']`

### GsSession._get_web_domain(self) -> str
Purpose: Return the Marquee web domain for the current environment.

**Algorithm:**
1. Load environment config
2. Return `env_config['MarqueeWebDomain']`

### GsSession._config_for_environment(cls, environment) -> ConfigParser section (classmethod)
Purpose: Lazily load and return config.ini section for the given environment.

**Algorithm:**
1. Branch: `cls.__config is None`:
   - Create `ConfigParser()`
   - Read `config.ini` from the same directory as the session.py source file
2. Return `cls.__config[environment]`

### GsSession.use(cls, environment_or_domain=Environment.PROD, client_id=None, client_secret=None, scopes=(), api_version=API_VERSION, application=DEFAULT_APPLICATION, http_adapter=None, use_mds=False, domain=Domain.APP) -> None (classmethod)
Purpose: Convenience method to create, initialize, and set as current session.

**Algorithm:**
1. Resolve `environment_or_domain`: if `Environment` instance -> `.name`, else keep as string
2. Branch: `domain is None` -> raise `MqError("None is not a valid domain.")`
3. Branch: `use_mds` -> override `domain = Domain.MDS_US_EAST`
4. Call `cls.get(...)` to create session instance
5. Call `session.init()`
6. Set `cls.current = session` (uses ContextMeta property setter -> thread-local storage)

### GsSession.get(cls, environment_or_domain=Environment.PROD, client_id=None, client_secret=None, scopes=(), token='', is_gssso=False, is_marquee_login=False, api_version=API_VERSION, application=DEFAULT_APPLICATION, http_adapter=None, application_version=APP_VERSION, domain=Domain.APP) -> GsSession (classmethod)
Purpose: Factory method returning the appropriate session subclass based on credentials provided.

**Algorithm:**
1. Resolve `environment_or_domain` to string name
2. Branch: `client_id is not None`:
   - Branch: `isinstance(scopes, str)` -> wrap in tuple
   - Merge scopes with `Scopes.get_default()` via `itertools.chain` + `set` + `tuple`
   - Return `OAuth2Session(...)`
3. Branch: `token` is truthy:
   - Branch: `is_gssso`:
     - Try: return `PassThroughGSSSOSession(...)`
     - Catch `NameError` -> raise `MqUninitialisedError('This option requires gs_quant_auth to be installed')`
   - Branch: `is_marquee_login` -> return `MQLoginSession(...)`
   - Branch: else -> return `PassThroughSession(...)`
4. Branch: else (no client_id, no token):
   - Try: return `MQLoginSession(...)`
   - Catch `NameError` -> raise `MqUninitialisedError('Unable to obtain MarqueeLogin token...')`

### GsSession.is_internal(self) -> bool
Purpose: Returns whether the session represents an internal user.

**Algorithm:**
1. Return `False` (subclasses may override)

### GsSession.sync (property) -> _SyncSessionAPI
Purpose: Lazy accessor for synchronous HTTP API facade.

**Algorithm:**
1. Branch: `self._sync_api is None` -> create `_SyncSessionAPI(self)`
2. Return `self._sync_api`

### GsSession.async_ (property) -> _AsyncSessionAPI
Purpose: Lazy accessor for async HTTP API facade.

**Algorithm:**
1. Branch: `self._async_api is None` -> create `_AsyncSessionAPI(self)`
2. Return `self._async_api`

### GsSession.post_to_activity_service(self) -> None
Purpose: Report session initialization to the GS activity tracking service.

**Algorithm:**
1. Build params dict: `featureApplication`, `gsQuantVersion`, `pythonVersion`
2. Try: POST to `{domain}/{api_version}/activities` with JSON body containing action, kpis, resource, parameters
3. Catch all exceptions -> pass (silently swallow)

### _SyncSessionAPI.get/post/put/delete(self, path, payload=None, request_headers=None, cls=None, include_version=True, timeout=DEFAULT_TIMEOUT, return_request_id=False, domain=None) -> Union[Base, tuple, dict]
Purpose: Thin delegation to `self._session._get/_post/_put/_delete`.
Note: `delete` also accepts `use_body: Optional[bool] = False`.

### _AsyncSessionAPI.get/post/put/delete(self, ...) -> Union[Base, tuple, dict] (async)
Purpose: Thin async delegation to `self._session._get_async/_post_async/_put_async/_delete_async`.
Note: `delete` also accepts `use_body: Optional[bool] = False`.

### _AsyncSessionAPI.connect_websocket(self, path: str, headers: Optional[dict] = None, include_version=True, domain: Optional[str] = None, **kwargs: Any) -> AsyncContextManager
Purpose: Async context manager delegating to `self._session._connect_websocket(...)`.

### OAuth2Session.__init__(self, environment, client_id, client_secret, scopes, api_version=API_VERSION, application=DEFAULT_APPLICATION, http_adapter=None, domain=Domain.APP) -> None
Purpose: Initialize OAuth2 session, resolve domain from config, optionally disable SSL verification.

**Algorithm:**
1. Branch: `environment` not in `(PROD.name, QA.name, DEV.name)`:
   - Load DEV config, set `url = environment` (treat as raw URL)
2. Branch: else -> load environment config, set `url = env_config[domain]`
3. Call `super().__init__(url, environment, ...)`
4. Set `auth_url`, `client_id`, `client_secret`, `scopes`, `_orig_domain = domain`
5. Branch: `environment == DEV.name` OR (`url != env_config['AppDomain']` AND `domain != Domain.MDS_US_EAST`):
   - Disable urllib3 InsecureRequestWarning
   - Set `self.verify = False`

### OAuth2Session._authenticate(self) -> None
Purpose: Perform OAuth2 client_credentials grant.

**Algorithm:**
1. Build `auth_data` dict: `grant_type='client_credentials'`, `client_id`, `client_secret`, `scope=' '.join(self.scopes)`
2. POST to `self.auth_url` with `data=auth_data`, `verify=self.verify`
3. Branch: `reply.status_code != 200` -> raise `MqAuthenticationError(reply.status_code, reply.text, context=self.auth_url)`
4. Parse JSON response, update session `Authorization` header with `'Bearer {access_token}'`

### OAuth2Session._headers(self) -> list[tuple[str, str]]
Purpose: Return only the Authorization header for WebSocket connections.

**Algorithm:**
1. Return `[('Authorization', self._session.headers['Authorization'])]`

### PassThroughSession.domain_and_verify(cls, environment_or_domain: str, domain: Optional[str]) -> tuple[str, bool] (classmethod)
Purpose: Resolve domain URL and SSL verify flag from config.

**Algorithm:**
1. Branch: `cls.__config is None` -> load `config.ini`
2. Set `verify = False`
3. Try: `domain = cls.__config[environment_or_domain][domain]`, set `verify = True`
4. Catch `KeyError` -> `domain = environment_or_domain` (treat as raw URL)
5. Return `(domain, verify)`

### PassThroughSession.__init__(self, environment: str, token, api_version=API_VERSION, application=DEFAULT_APPLICATION, http_adapter=None, domain=None) -> None
Purpose: Initialize with a pre-existing bearer token.

**Algorithm:**
1. Branch: `domain is not None` -> use as-is; Branch: `domain is None` -> default to `'AppDomain'`
2. Resolve via `self.domain_and_verify(environment, domain)`
3. Call `super().__init__()` with resolved domain and verify
4. Set `self._orig_domain = domain`, `self.token = token`

### PassThroughSession._authenticate(self) -> None
Purpose: Set bearer token on session headers.

**Algorithm:**
1. Update `self._session.headers` with `Authorization: Bearer {self.token}`

### PassThroughSession._headers(self) -> list[tuple[str, str]]
Purpose: Return Authorization header for WebSocket connections.

### PassThroughGSSSOSession.__init__(self, environment: str, token, api_version=API_VERSION, application=DEFAULT_APPLICATION, http_adapter=None, csrf_token=None) -> None
Purpose: Initialize with GSSSO token and optional CSRF token.

**Algorithm:**
1. Resolve domain and verify via `KerberosSessionMixin.domain_and_verify(environment)`
2. Call `GsSession.__init__(...)` directly (not super, due to MRO)
3. Set `self.token = token`, `self.csrf_token = csrf_token`

### PassThroughGSSSOSession._authenticate(self) -> None
Purpose: Set GSSSO and CSRF cookies/headers.

**Algorithm:**
1. Branch: `not (self.token and self.csrf_token)`:
   - Call `self._handle_cookies(self.token)` (from KerberosSessionMixin)
   - Return early
2. Branch: both token and csrf_token present:
   - Create and set `GSSSO` cookie on `.gs.com` domain
   - Branch: `self.csrf_token` is truthy:
     - Create and set `MARQUEE-CSRF-TOKEN` cookie
     - Update session header `X-MARQUEE-CSRF-TOKEN`

### MQLoginSession.__init__(self, environment_or_domain: str, domain: str = Domain.APP, api_version: str = API_VERSION, application: str = DEFAULT_APPLICATION, http_adapter=None, application_version=APP_VERSION, mq_login_token=None) -> None
Purpose: Initialize MarqueeLogin session.

**Algorithm:**
1. Resolve `selected_domain, verify` via `MQLoginMixin.domain_and_verify(environment_or_domain)`
2. Branch: `domain == Domain.MDS_WEB`:
   - Load environment config
   - Override `selected_domain = env_config[domain]`
3. Set `self.mq_login_token = mq_login_token`
4. Call `GsSession.__init__(...)` directly
5. Set `self._orig_domain = domain`

## State Mutation

- **`GsSession.__config` (class-level):** Lazily set on first call to `_config_for_environment()`. Once set, never cleared. Shared across all `GsSession` subclasses.
- **`PassThroughSession.__config` (class-level):** Separate config cache from `GsSession.__config`. Lazily set on first `domain_and_verify()` call.
- **`CustomHttpAdapter.__ssl_ctx` (class-level):** Lazily set on first `ssl_context()` call. Singleton for process lifetime.
- **`GsSession.current` (thread-local via `ContextMeta`):** The "current" session singleton stored in `threading.local()`. Set by `GsSession.use()` or by entering a context manager (`with session:`). Thread-safe: each thread has its own `current`. Supports nesting via a path tuple (stack).
- **`self._session`:** Created in `init()`, set to `None` in `close()` or on context exit if `__close_on_exit` is True.
- **`self._session_async`:** Created in `_init_async()`, closed in `_on_aexit()` or `close()`.
- **`self._sync_api` / `self._async_api`:** Lazily created on first property access; never cleared.
- **Authentication headers/cookies on `self._session`:** Set by `_authenticate()` implementations; refreshed on 401 retry.
- **Thread safety:** The `ContextBase` metaclass uses `threading.local()` for the session path stack, making `GsSession.current` thread-safe. However, `_session` and `_session_async` instances on a `GsSession` object are not themselves thread-safe -- shared adapter instances require external synchronization.

### Elixir GenServer Mapping

The Python singleton pattern (`ContextMeta` + thread-local) maps to an Elixir GenServer:
- `GsSession.current` -> `GenServer.call(SessionServer, :get_current)`
- `GsSession.use(...)` -> `GenServer.call(SessionServer, {:use, opts})`
- Context manager enter/exit -> `GenServer.call(SessionServer, {:push, session})` / `{:pop}`
- Thread-local isolation -> per-process state (each Elixir process has its own process dictionary or dedicated GenServer)
- `_session` / `_session_async` -> HTTP client pools managed as GenServer state (e.g., Finch pools)
- Authentication tokens -> stored in GenServer state, refreshed on 401

## Error Handling

| Exception | Raised By | Condition |
|-----------|-----------|-----------|
| `NotImplementedError` | `GsSession._authenticate()` | Base class abstract method called directly |
| `MqAuthenticationError` | `OAuth2Session._authenticate()` | OAuth2 token endpoint returns non-200 status |
| `MqRequestError` | `GsSession.__request()` | HTTP 401 on retry (`try_auth=False`) |
| `MqRequestError` | `GsSession.__request_async()` | HTTP 401 on retry (`try_auth=False`) |
| `error_builder(status, ...)` | `GsSession._parse_response()` | Any non-2xx response: returns `MqAuthenticationError` (401), `MqAuthorizationError` (403), `MqRateLimitedError` (429), `MqInternalServerError` (500), `MqTimeoutError` (504), or generic `MqRequestError` |
| `MqError` | `GsSession._build_request_params()` | Unrecognized HTTP method/use_body combination |
| `MqError` | `GsSession.use()` | `domain is None` |
| `MqUninitialisedError` | `GsSession.get()` | `is_gssso=True` but `PassThroughGSSSOSession` not defined (gs_quant_auth not installed) |
| `MqUninitialisedError` | `GsSession.get()` | No credentials provided and `MQLoginSession` not defined |
| `MqUninitialisedError` | `ContextMeta.current` (getter) | `GsSession.current` accessed when no session is set |
| `MqValueError` | `ContextMeta.current` (setter) | Attempt to set `current` while in a nested context |

## Edge Cases

- **OpenSSL version branching in `__init__`:** When `http_adapter` is `None`, the adapter chosen depends on `ssl.OPENSSL_VERSION_INFO >= (3, 0, 0)`. OpenSSL 3.x gets `CustomHttpAdapter` with legacy server connect; older gets standard adapter with `pool_maxsize=100`.
- **Proxies iteration:** The `mounts` dict comprehension iterates `proxies` as key-value pairs (`for key, val in proxies`). This expects proxies to be an iterable of 2-tuples, not a dict. If a dict is passed, it iterates over keys only and will fail with a `ValueError` (not enough values to unpack).
- **`_orig_domain` overwrite:** Both `GsSession.__init__` and subclass `__init__` set `_orig_domain`. The subclass value wins. `OAuth2Session` sets it to the `domain` parameter (e.g., `Domain.APP`), not the resolved URL.
- **Environment resolution fallback:** If `environment` string is not a valid `Environment` name AND `domain` is not an `Environment` instance, defaults to `Environment.DEV`. This means passing a raw URL as domain with a non-standard environment string silently sets `Environment.DEV`.
- **OAuth2 DEV environment verify bypass:** `OAuth2Session.__init__` disables SSL verification for DEV environment OR any non-AppDomain URL (except MDS_US_EAST). The condition `url != env_config['AppDomain'] and not domain == Domain.MDS_US_EAST` means MDS_WEB domain URLs also disable verification.
- **`__del__` calling `close()`:** The destructor calls `close()`, which tries `asyncio.run()` for the async session. If called during interpreter shutdown or from a thread with a running event loop, this can raise `RuntimeError` -- but it is swallowed by the bare `except Exception: pass`.
- **401 retry is single-attempt:** Both `__request` and `__request_async` retry at most once on 401 (recursive call with `try_auth=False`).
- **Sync vs async data_key:** `__request` passes `data_key="data"` (for `requests`), `__request_async` passes `data_key="content"` (for `httpx`). This is a subtle API difference between the two HTTP libraries.
- **Conditional class definitions:** `KerberosSession`, `PassThroughGSSSOSession`, and `MQLoginSession` are defined inside `try/except ModuleNotFoundError` blocks. If `gs_quant_auth` is not installed, these classes do not exist at all, and referencing them raises `NameError` (caught in `GsSession.get()`).
- **`_authenticate` backoff decorators:** The `@backoff.on_exception` and `@backoff.on_predicate` decorators on the abstract `_authenticate` are applied to the base class. Due to Python MRO, subclass overrides of `_authenticate` do NOT inherit these decorators -- the backoff only applies if `_authenticate` is called via `super()` chain or the base descriptor.
- **`post_to_activity_service` swallows all errors:** Any exception during the activity POST is silently ignored.
- **Async session cookie sync:** `_authenticate_async` iterates `self._session.cookies` and copies them by name/value/domain. httpx cookies API differs from requests, so cookie attributes beyond name/value/domain are lost.
- **WebSocket URL construction:** When no domain is given, `_connect_websocket_raw` does `'ws' + self.domain[4:]` which assumes `self.domain` starts with `http` (4 chars). If domain starts with `https`, this produces `wss://...` correctly. But if the domain format is unexpected, the URL will be malformed.

## Coverage Notes

- Branch count: ~65 distinct branch points
- Key branches to cover:
  - `__init__` environment resolution: 3 branches (valid name, Environment instance, fallback DEV)
  - `__init__` http_adapter: 3 branches (None+OpenSSL3, None+older, provided)
  - `_build_request_params`: GET/DELETE no-body vs POST/PUT/body vs else (MqError); sub-branches for tracing, headers, msgpack vs json vs raw
  - `_parse_response`: error vs content-type present (msgpack vs json vs other) vs no content-type; unpack sub-branches
  - `__request`/`__request_async`: 401 retry vs 401 fail vs success; tracing vs no-tracing
  - `GsSession.get()`: OAuth2 vs token+gssso vs token+marquee_login vs token+plain vs no-creds
  - `OAuth2Session.__init__`: known env vs custom URL; verify disable conditions
  - `PassThroughGSSSOSession._authenticate`: token+csrf vs handle_cookies path
  - `_connect_websocket_raw`: websockets version >= 14 vs < 14; domain provided vs not; wss vs ws
  - `_headers`: each cookie present/absent combination
  - Conditional class definitions: gs_quant_auth installed vs not
- Pragmas: None observed in this file
- The `@backoff` decorators on `_authenticate` add implicit retry branches that are difficult to test directly
