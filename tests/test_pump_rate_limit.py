"""Pump HTTP 429s that keep coming pause every read of that host (5 s, 10, 20 ... 60 s); a short overrun does not."""
import pytest

from src.data import fast_get as F

U = "https://advanced-indexer.pump.fun/boards/new"


@pytest.fixture(autouse=True)
def _clean():
    for d in (F._rate_hits, F._rate_until, F._rate_logged):
        d.clear()
    yield
    for d in (F._rate_hits, F._rate_until, F._rate_logged):
        d.clear()


def test_a_few_429s_pause_nothing():
    assert F._rate_limited(U) == 0 and F._rate_limited(U) == 0
    assert not F._cooling(U, "mayhem2board") and F.rate_limit_left() == 0


def test_repeated_429s_pause_every_lane_and_grow():
    pauses = [F._rate_limited(U) for _ in range(8)]
    assert pauses == [0, 0, 5, 10, 20, 40, 60, 60]
    assert F._cooling(U, "mayhem2board") and F._cooling(U, "callout") and F._cooling(U, "")
    assert not F._cooling("https://frontend-api-v3.pump.fun/x", "callout")       # another host is not paused
    with pytest.raises(Exception) as ex:
        F._read(U, 1.0, "shared")                                            # no request is sent while paused
    assert "429" in str(ex.value)


def test_good_answers_take_429s_off_the_count():
    F._rate_limited(U); F._rate_limited(U)
    F._rate_ok(U); F._rate_ok(U)
    assert F._rate_limited(U) == 0 and F._rate_limited(U) == 0                  # still under the free count
