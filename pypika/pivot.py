from typing import List, Tuple, Type

from pypika import Case, Field, NULL
import pypika.functions as fn


def pivot(
    pivot_column: Field,
    value_column: Field,
    pivot_values: List[str],
    aggregate_function: Type[fn.AggregateFunction],
) -> Tuple[fn.AggregateFunction]:
    return (
        aggregate_function(Case().when(pivot_column == pivot_value, value_column).else_(NULL)).as_(pivot_value)
        for pivot_value in pivot_values
    )
