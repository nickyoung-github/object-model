from dataclasses import dataclass
import datetime as dt
from enum import StrEnum, auto
from pathlib import Path
from typing import Union

from object_model import (
    Discriminated,
    Identifiable,
    IdField,
    Reference,
    TypeRegistry,
    Unique,
)

__registry = TypeRegistry()


@dataclass(kw_only=True)
class Counterparty(Identifiable):
    name: IdField[str]
    location: str
    contact_name: str

__registry.register("Counterparty", Counterparty)


@dataclass(kw_only=True)
class CustomerBook(Identifiable):
    counterparty: IdField[Counterparty]
    sales_coverage: tuple[str, ...]
    
__registry.register("CustomerBook", CustomerBook)

    
    
@dataclass(kw_only=True)
class TradingBook(Identifiable):
    name: IdField[str]
    owner: str
    
__registry.register("TradingBook", TradingBook)

    
@dataclass(kw_only=True)
class Option(Unique):
    ticker: str
    strike: float
    maturity: dt.datetime
    
__registry.register("Option", Option, Path("Instrument"))

Instrument = Discriminated[Path("Instrument")]


class TradeAction(StrEnum):
    Buy = auto()
    Sell = auto()
    Pay = auto()
    Receive = auto()
    
 
@dataclass(kw_only=True)
class Position(Unique):
    quantity: float
    action: TradeAction
    instrument: Reference[Instrument]

__registry.register("Position", Position)


@dataclass(kw_only=True)
class Deal(Identifiable):
    deal_id: IdField[str]
    book_from: Reference[TradingBook]
    book_to: Reference[TradingBook | CustomerBook]
    position: list[Position]
    
__registry.register("Deal", Deal)
