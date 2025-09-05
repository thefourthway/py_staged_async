import contextlib
import inspect
import types
import typing


class _ApprovalUnset:
    pass


class _DeadGenerator:
    pass


class _ItemUnset:
    pass


class _ResultUnset:
    pass


# The final return value of our SAO, stored in SAO._result
_R = typing.TypeVar('_R')

_RCV = typing.TypeVar('_RCV', covariant=True)

# The generator returned by the client function
AsyncClientEventGeneratorT: typing.TypeAlias = "typing.AsyncGenerator[_OperationEvent | _R, _OperationEvent]"
SyncClientEventGeneratorT: typing.TypeAlias = "typing.Generator[_OperationEvent | _R, _OperationEvent, None]"

SAOOriginalClientGenerator: typing.TypeAlias = 'AsyncClientEventGeneratorT[_R] | SyncClientEventGeneratorT[_R]'

# The event iterator type for SAO.__call__
SAOEventIterator: typing.TypeAlias = "typing.AsyncIterator[_OperationEvent]"

SAOEnabledEventGenerator: typing.TypeAlias = 'typing.Callable[..., SAOEventIterator]'

# The generator returned by SAOGenerator.generate
AsyncEventGeneratorT: typing.TypeAlias = "typing.AsyncGenerator[_OperationEvent, None]"

# The stored value of the previous item returned by the client generator. type[_ItemUnset] is the initial state of SAOGenerator._previous_yield
_GeneratorItemState: typing.TypeAlias = "_OperationEvent | _R | type[_ItemUnset]"

# The type of the function we are passing our SAO into

class SAOClientCallable(typing.Protocol[_RCV]):
    def __call__(self, *args: typing.Any, **kwargs: typing.Any) -> 'SAOOriginalClientGenerator[_RCV]':
        ...

SAOResultAwaitable: typing.TypeAlias = 'typing.Awaitable[_R]'

class _OperationEvent:
    def __init__(self, stage_id: str, event_id: str, event_args: dict[str, typing.Any], stage_context: dict[str, typing.Any]):
        self._stage_id = stage_id
        self._event_id = event_id
        self._event_args: dict[str, typing.Any] = event_args
        self._stage_context = stage_context

        self._approval_status: type[_ApprovalUnset] | bool = _ApprovalUnset
        self._allow_args: tuple[typing.Any, ...] | None = None

    def approve(self, *args: typing.Any):
        self._approval_status = True
        self._allow_args = args

    def cancel(self):
        self._approval_status = False
        self._allow_args = None

    @property
    def stage_id(self) -> str:
        return self._stage_id

    @property
    def event_id(self) -> str:
        return self._event_id

    @property
    def event_args(self):
        return self._event_args

    @property
    def stage_context(self):
        return self._stage_context

    @property
    def caller_args(self) -> tuple[typing.Any, ...] | None:
        return self._allow_args

    @property
    def has_been_approved(self) -> bool:
        return self._approval_status is True

    @property
    def has_been_stamped(self) -> bool:
        return self._approval_status != _ApprovalUnset

    @property
    def combined_id(self):
        return f'{self.stage_id}.{self.event_id}'

    def __hash__(self) -> int:
        return hash(self._stage_id)

    def __repr__(self) -> str:
        return f'OperationEvent(id={repr(self._stage_id)}, kwargs={repr(self.event_args)})'

    def __bool__(self):
        return self.has_been_approved

    def __str__(self):
        return repr(self)


class _OperationStage:
    """
    Usage:

    with sao.stage('DoSomething', start_account_id=1) as stage:
        ev = yield stage.allowed("try_set_db_field", field_name='account_id', field_value=new_account_id)

    """

    def __init__(self, stage_id: str, stage_context: dict[str, typing.Any]):
        self._id = stage_id
        self._context = stage_context

    def allowed(self, event_id: str, **kwargs: typing.Any):
        return _OperationEvent(self._id, event_id, kwargs, self._context)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


class _SyncGenAdapter(typing.AsyncGenerator["_OperationEvent | _R", "_OperationEvent"]):
    def __init__(self, gen: "SyncClientEventGeneratorT[_R]") -> None:
        self._gen = gen

    def __aiter__(self) -> "typing.AsyncIterator[_OperationEvent | _R]":
        return self

    async def __anext__(self) -> "_OperationEvent | _R":
        try:
            return next(self._gen)
        except StopIteration:
            raise StopAsyncIteration

    # type: ignore[override]
    async def asend(self, value: "_OperationEvent") -> "_OperationEvent | _R":
        try:
            return self._gen.send(value)
        except StopIteration:
            raise StopAsyncIteration

    # type: ignore[override]
    async def athrow(self, typ, val=None, tb=None) -> "_OperationEvent | _R":
        try:
            return self._gen.throw(typ, val, tb)
        except StopIteration:
            raise StopAsyncIteration

    async def aclose(self) -> None:  # type: ignore[override]
        self._gen.close()

def _get_original_sao_fn_bound(callable_obj: typing.Any) -> 'SAOClientCallable[object] | None':
    """
    Return the original SAO client callable, rebound to the same instance/class
    as `callable_obj` if it is a bound method. Returns None if no marker found.
    """
    inst = getattr(callable_obj, "__self__", None)      # instance or class (for classmethod)
    cur: typing.Any = callable_obj
    while True:
        orig = getattr(cur, "__original_sao_fn__", None)
        if orig is not None:
            break
        cur = getattr(cur, "__wrapped__", None)
        if cur is None:
            return None
        
    return typing.cast('SAOClientCallable[object]', types.MethodType(orig, inst)) if inst is not None else typing.cast('SAOClientCallable[object]', orig)

class _SAOGenerator(typing.Generic[_R]):
    def __init__(self, sao: 'SAO[_R]', fn: 'SAOClientCallable[_R]', *args: typing.Any, **kwargs: typing.Any):
        self._sao: 'SAO[_R] | None' = sao
        self._fn: 'SAOClientCallable[_R] | None' = fn
        self._fn_args: 'tuple[typing.Any, ...] | None' = args
        self._fn_kwargs: 'dict[str, typing.Any] | None' = kwargs

        self._fn_async_generator: 'AsyncClientEventGeneratorT[_R] | type[_DeadGenerator] | None' = None
        self._previous_yield: '_GeneratorItemState[_R]' = _ItemUnset

    def _invalidate(self):
        self._sao = None
        self._fn = None
        self._fn_args = None
        self._fn_kwargs = None
        self._fn_async_generator = _DeadGenerator
        self._previous_yield = _ItemUnset

    async def generate(self) -> AsyncEventGeneratorT:
        if self._sao is None or self._fn is None or self._fn_args is None or self._fn_kwargs is None:
            raise RuntimeError(
                'The SAO that can be re-entered is not the true SAO')

        if self._fn_async_generator == _DeadGenerator:
            raise RuntimeError(
                'The SAO that can be re-entered is not the true SAO'
            )

        if self._fn_async_generator is None:
            raw = self._fn(
                *self._fn_args,
                **self._fn_kwargs
            )

            if inspect.isgenerator(raw):
                self._fn_async_generator = _SyncGenAdapter(
                    typing.cast("SyncClientEventGeneratorT[_R]", raw))
            else:
                # assume already an async generator
                self._fn_async_generator = typing.cast(
                    "AsyncClientEventGeneratorT[_R]", raw)

        gen: 'AsyncClientEventGeneratorT[_R]' = typing.cast(
            'AsyncClientEventGeneratorT[_R]',
            self._fn_async_generator
        )

        item: _OperationEvent | _R
        send: _OperationEvent | None = None
        closed: bool = False

        try:
            while True:
                try:
                    item = await (anext(gen) if send is None else gen.asend(send))
                    send = None
                except StopAsyncIteration:
                    break

                self._previous_yield = item

                if isinstance(item, _OperationEvent):
                    yield item

                    if not item.has_been_stamped:
                        item.approve()

                    if item.has_been_approved:
                        send = item
                    else:
                        with contextlib.suppress(Exception):
                            await gen.aclose()

                        closed = True
                        break
        except Exception:
            if not closed:
                with contextlib.suppress(Exception, RuntimeError):
                    await gen.aclose()
                closed = True

            raise
        finally:
            if not closed:
                with contextlib.suppress(Exception, RuntimeError):
                    await gen.aclose()

                closed = True
                
            final_item = self._previous_yield

            if not isinstance(final_item, _OperationEvent) and final_item != _ItemUnset:
                # Just hope for the best that the type is _R, can't really do much about it
                self._sao._result = typing.cast(_R, final_item)

            self._invalidate()


class SAO(typing.Generic[_R]):
    def __init__(self) -> None:
        self._result: '_R | type[_ResultUnset]' = _ResultUnset

        self._sao_generator_obj: '_SAOGenerator[_R] | None' = None
        self._event_generator: 'AsyncEventGeneratorT | None' = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self._invalidate_generator()

    def stage(self, stage_id: str, **kwargs: typing.Any) -> _OperationStage:
        return _OperationStage(stage_id, kwargs)

    def result(self) -> _R:
        if self._result == _ResultUnset:
            raise RuntimeError(
                'No result was returned by the client generator'
            )

        return typing.cast(_R, self._result)

    def _invalidate_generator(self):
        self._sao_generator_obj = None
        self._event_generator = None

    def __call__(self, fn: 'typing.Callable[..., typing.AsyncIterator[_OperationEvent]]', *args: typing.Any, **kwargs: typing.Any) -> 'SAOEventIterator':
        if self._sao_generator_obj is not None or self._event_generator is not None:
            raise RuntimeError('SAO has already been initialized')

        if self._result != _ResultUnset:
            self._result = _ResultUnset

        async def gen():
            if self._sao_generator_obj is not None or self._event_generator is not None:
                raise RuntimeError(
                    'SAO may only have one iterator client at a time')

            real_fn: 'SAOClientCallable[_R] | None' = typing.cast('SAOClientCallable[_R] | None', _get_original_sao_fn_bound(fn))

            if real_fn is None:
                raise RuntimeError(
                    'The SAO that is not decorated is not the true SAO'
                )

            self._sao_generator_obj = _SAOGenerator(
                self, real_fn, *args, **kwargs
            )

            self._event_generator = self._sao_generator_obj.generate()

            try:
                async for item in self._event_generator:
                    yield item
            except Exception:
                raise
            finally:
                self._invalidate_generator()

        return gen()


_current_sao: SAO[typing.Any] | None = None


def make_sao() -> 'SAO[object]':
    sao = SAO[object]()
    global _current_sao
    _current_sao = sao
    return sao


def load_sao():
    global _current_sao

    if _current_sao is not None:
        # could this be the true SAO?
        sao, _current_sao = _current_sao, None
        return typing.cast('SAO[object]', sao)

    return SAO[object]()


async def _complete_sao_call(fn: 'SAOEnabledEventGenerator', *args: typing.Any, **kwargs: typing.Any):
    sao = make_sao()

    async for event in sao(fn, *args, **kwargs):
        event.approve()

    return sao.result()


def iterate_sao(fn: 'SAOEnabledEventGenerator', *args: typing.Any, **kwargs: typing.Any) -> 'SAOEventIterator':
    sao = make_sao()

    return sao(fn, *args, **kwargs)


def fold_sao(fn: 'SAOEnabledEventGenerator', *args: typing.Any, **kwargs: typing.Any) -> 'SAOResultAwaitable[_R]':
    return typing.cast('SAOResultAwaitable[_R]', _complete_sao_call(fn, *args, **kwargs))


def enable_sao(fn: 'SAOClientCallable[_R]') -> 'typing.Callable[..., SAOEventIterator]':
    """
    This will provide an extra overload for fn so that it can be called either as an event generator, or an awaitable
    that returns sao.result when the generator completes.
    """
    def wrap_factory(realized_fn: 'SAOClientCallable[_R]') -> 'typing.Callable[..., SAOEventIterator]':
        """
        @param: realized_fn - The SAO that can be realized is not the true SAO
        """
        def cast_sao_function(*args: typing.Any, **kwargs: typing.Any) -> 'SAOEventIterator':

            return typing.cast('SAOEventIterator', realized_fn(*args, **kwargs))

        setattr(cast_sao_function, '__original_sao_fn__', realized_fn)
        return cast_sao_function

    return wrap_factory(fn)
