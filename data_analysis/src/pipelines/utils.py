from typing import Any
from isda_streaming.data_stream import (
    DataStream,
    TimedStream,
    WindowedStream,
    KeyedStream,
)
from isda_streaming.synopsis import CountMinSketch, BloomFilter, ReservoirSample

def _filter_pkw(stream_element: tuple) -> bool:
    if stream_element[2] == 'pkw':
        return True
    return False

def _key_by_lane(stream_element: tuple) -> int:
    return stream_element[0]

def _get_velocity(stream_element: tuple) -> float:
    return stream_element[1]

def _get_max(velocity_1: float, velocity_2: float) -> float:
    if velocity_1 > velocity_2:
        return velocity_1
    return velocity_2
# -----------------------------------------------------------------------------
def _filter_lane_1_and_2(x):
    if x[0] == 3.0:
        return False
    return True

def _map_lkw_to_count(stream_element: tuple) -> tuple:
    if stream_element[2] == "lkw":
        return (1, 1)
    return (0, 1)

def _count_lkw_count_all(stream_element_1: tuple, stream_element_2: tuple) -> tuple:
    return (
        stream_element_1[0] + stream_element_2[0],
        stream_element_1[1] + stream_element_2[1],
    )

def _cal_percent(stream_element: tuple) -> tuple:
    percent = (stream_element[0] / stream_element[1]) * 100
    return round(percent, 2)