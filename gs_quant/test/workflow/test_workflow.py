"""
Copyright 2023 Goldman Sachs.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from typing import Optional, Tuple

from dataclasses_json import global_config


def test_global_decoders_registered():
    """Verify that importing workflow registers the global decoders for HedgeTypes."""
    from gs_quant.target.workflow_quote import HedgeTypes
    from gs_quant.json_convertors import decode_hedge_type, decode_hedge_types

    # Import the workflow module which registers decoders
    import gs_quant.workflow.workflow  # noqa

    assert global_config.decoders[Optional[HedgeTypes]] is decode_hedge_type
    assert global_config.decoders[HedgeTypes] is decode_hedge_type
    assert global_config.decoders[Optional[Tuple[HedgeTypes, ...]]] is decode_hedge_types
