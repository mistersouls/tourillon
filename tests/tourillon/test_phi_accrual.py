# Copyright 2026 Tourillon Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

from tourillon.core.probe.phi import PhiAccrual


@pytest.mark.tourillon
def test_phi_failure_detector_validates_constructor_and_threshold() -> None:
    with pytest.raises(ValueError, match="max_samples must be >= 1"):
        PhiAccrual(max_samples=0)

    with pytest.raises(ValueError, match="min_stddev must be > 0"):
        PhiAccrual(min_stddev=0)

    with pytest.raises(ValueError, match="acceptable_heartbeat_pause must be >= 0"):
        PhiAccrual(acceptable_heartbeat_pause=-0.1)

    with pytest.raises(ValueError, match="min_samples must be >= 2"):
        PhiAccrual(min_samples=1)


@pytest.mark.tourillon
def test_phi_is_zero_until_enough_observations_then_increases_with_elapsed_time() -> (
    None
):
    detector = PhiAccrual(min_samples=3, min_stddev=0.05)

    detector.record(0.0)
    detector.record(1.0)
    detector.record(2.0)
    assert detector.phi(now=2.1) == 0.0

    detector.record(3.0)
    near = detector.phi(now=3.2)
    far = detector.phi(now=5.0)

    assert near >= 0.0
    assert far > near


@pytest.mark.tourillon
def test_phi_with_high_variance_is_less_suspicious_for_same_elapsed() -> None:
    steady = PhiAccrual(min_samples=2, min_stddev=0.1)
    bursty = PhiAccrual(min_samples=2, min_stddev=0.1)

    for ts in (0.0, 1.0, 2.0, 3.0, 4.0):
        steady.record(ts)

    for ts in (0.0, 0.2, 2.0, 2.3, 4.0):
        bursty.record(ts)

    steady_phi = steady.phi(now=5.2)
    bursty_phi = bursty.phi(now=5.2)

    assert steady_phi > bursty_phi


@pytest.mark.tourillon
def test_reset_clears_state() -> None:
    detector = PhiAccrual(min_samples=2)
    detector.record(0.0)
    detector.record(1.0)

    assert detector.interval_samples == 1

    detector.reset()

    assert detector.interval_samples == 0
    assert detector.has_observations is False
    assert detector.phi(now=2.0) == 0.0
