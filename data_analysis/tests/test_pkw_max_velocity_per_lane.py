import pytest

from pipelines.transform import pkw_max_velocity_per_lane
import _old_solutions

from isda_streaming.data_stream import KeyedStream, TimedStream

data_fpath = "data/autobahn.csv"

def test_pkw_max_velocity_per_lane():
    start = 0
    end = 30
    input_stream = TimedStream()
    input_stream.from_csv(data_fpath, start, end)

    assert pkw_max_velocity_per_lane(input_stream) == _old_solutions.pkw_max_velocity_per_lane(input_stream)