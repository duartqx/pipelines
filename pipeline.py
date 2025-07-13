import asyncio

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    AsyncIterator,
    Callable,
    Coroutine,
    Sequence,
    Type,
    cast,
)
from typing_extensions import override

from logger import slogger


type Item = int


@dataclass(frozen=True)
class Context:
    uow: str
    data: int


@dataclass(frozen=True)
class PStep[T]:
    name: str
    handler: Callable[[Context, T], Coroutine[None, None, T]]

    async def __call__(self, ctx: Context, item: T) -> T:
        await slogger(
            "INFO", step=self.name, handler=self.handler.__name__, handling=item
        )

        try:
            result = await self.handler(ctx, item)
            await slogger(
                "INFO",
                step=self.name,
                handler=self.handler.__name__,
                handled=item,
                result=result,
            )
            return result
        except Exception as e:
            await slogger(
                "ERROR",
                step=self.name,
                handler=self.handler.__name__,
                exc_type=e.__class__.__name__,
                exc=str(e),
            )
            raise e


class PCollection[T](ABC):
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[T]: ...


class ItemCollection(PCollection[Item]):
    @override
    def __aiter__(self) -> AsyncIterator[Item]:
        async def generator() -> AsyncIterator[Item]:
            for val in [2, 3, 10, 4]:
                yield val

        return generator()


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
            tasks = [self.apply(item) async for item in self.collection(ctx=self.ctx)]

            for result in await asyncio.gather(*tasks, return_exceptions=True):
                if not isinstance(result, Exception):
                    yield cast(T, result)

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


async def main():
    pipeline = Pipeline[Item](
        name="pipeline1",
        ctx=Context(data=1, uow="uow"),
        collection=ItemCollection,
        steps=[
            PStep(name="step1", handler=step1),
            PStep(name="step2", handler=step2),
        ],
    )

    print(await pipeline.result())


asyncio.run(main())
