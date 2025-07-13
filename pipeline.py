import asyncio

from abc import ABC, abstractmethod
from dataclasses import dataclass

from typing import (
    AsyncIterator,
    Callable,
    Coroutine,
    Sequence,
    Type,
)
from typing_extensions import override

from logger import SLogger, SLoggable, Slog


type Item = int


@dataclass(frozen=True)
class Context:
    uow: str
    data: int


class PStep[T]:
    def __init__(
        self, name: str, handler: Callable[[Context, T], Coroutine[None, None, T]]
    ) -> None:
        self.name = name
        self.handler = handler

    def slog(self) -> Slog:
        return {
            "step": self.name,
            "handler": str(self.handler.__name__),
        }

    async def __call__(self, ctx: Context, item: T) -> T:
        async with SLogger(self.slog()) as slogger:
            return await slogger(await self.handler(ctx, item))


class PCollection[T](ABC, SLoggable):
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx

    def slog(self) -> Slog:
        return {"collection": self.__class__.__name__}

    def __aiter__(self) -> AsyncIterator[T]:
        slogger = SLogger(self.slog(), skip_enter=True)

        async def _() -> AsyncIterator[T]:
            async for item in self.sequence():
                async with slogger:
                    yield await slogger(item)

        return _()

    @abstractmethod
    async def sequence(self) -> AsyncIterator[T]:
        for val in []:
            yield val


class ItemCollection(PCollection[Item]):
    @override
    async def sequence(self) -> AsyncIterator[Item]:
        for val in [2, 3, 10, 4, 5, 1, 3, 12, -33, 8, 4, 2, -11]:
            yield val


class Pipeline[T]:
    def __init__(
        self,
        name: str,
        ctx: Context,
        collection: Type[PCollection[T]],
        steps: Sequence[PStep],
    ) -> None:
        self.name = name
        self.ctx = ctx
        self.collection = collection
        self.steps = steps

    def __aiter__(self) -> AsyncIterator[T]:
        async def generator() -> AsyncIterator[T]:
            for result in await asyncio.gather(
                *[self.apply(item) async for item in self.collection(ctx=self.ctx)],
                return_exceptions=True,
            ):
                if not isinstance(result, BaseException):
                    yield result

        return generator()

    async def apply(self, item: T) -> T:
        result = item
        for step in self.steps:
            result = await step(self.ctx, result)
        return result

    async def result(self) -> list[T]:
        return [item async for item in self]


async def step1(ctx: Context, item: Item) -> Item:
    if item > 5:
        raise ValueError(f"Valor ({item}) nao permitido por ser maior que 5")
    return item * 2


async def step2(ctx: Context, item: Item) -> Item:
    return item + 3


async def step3(ctx: Context, item: Item) -> Item:
    if item < 0:
        raise ValueError(f"Valor ({item}) nao permitido por ser menor que 0")
    return item * 7


async def main():
    pipeline = Pipeline[Item](
        name="pipeline1",
        ctx=Context(data=1, uow="uow"),
        collection=ItemCollection,
        steps=[
            PStep(name="step1", handler=step1),
            PStep(name="step2", handler=step2),
            PStep(name="step3", handler=step3),
            PStep(name="step4", handler=step2),
            PStep(name="step5", handler=step2),
            PStep(name="step6", handler=step3),
        ],
    )

    print(await pipeline.result())


asyncio.run(main())
