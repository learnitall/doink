"""Test functionality of stuff in state.py."""

import json

import pytest

from doink.state import ObjectUpdate


class TestObjectUpdate:
    """Test functionality of the ObjectUpdate class."""

    @staticmethod
    @pytest.fixture
    def obj_test_values():
        """
        Return dict of values to test serialization.

        The returned values are meant to be attached to the
        ObjectUpdate's value field. Items under the 'good' key will
        have no problem being serialized and deserialized. Items
        under the 'bad' key should raise an error.
        """

        primitives = ["str", 0, 1.0, [1], (3,), True, False, None]
        good_values = [{"str": v for v in primitives}]

        bad_values = (
            primitives
            + [{v: "str" for v in primitives if getattr(v, "__hash__", False)}]
            + [{"str": v for v in good_values}]
            + [
                {
                    "str": ObjectUpdate(
                        owner_uid="bad", obj_uid="bad", value=dict()
                    )
                }
            ]
        )

        return {
            "good": good_values,
            "bad": bad_values,
        }

    def test_object_update_can_be_serialized_to_json(self, obj_test_values):
        """Test ObjectUpdate's validation for the 'value' field."""

        for i, value in enumerate(obj_test_values["good"]):
            update = ObjectUpdate(
                obj_uid=str(i), owner_uid=str(i), value=value
            )
            expected = json.dumps(
                {"obj_uid": str(i), "owner_uid": str(i), "value": value}
            )
            result = update.to_json()

            assert result == expected

        for i, value in enumerate(obj_test_values["bad"]):
            with pytest.raises(TypeError):
                ObjectUpdate(obj_uid=str(i), owner_uid=str(i), value=value)

    def test_object_update_can_be_deserialized_from_json(
        self, obj_test_values
    ):
        """Test ObjectUpdate's validation for the 'value' field."""

        for i, value in enumerate(obj_test_values["good"]):
            expected = ObjectUpdate(
                obj_uid=str(i), owner_uid=str(i), value=value
            )
            result = ObjectUpdate.from_json(
                json.dumps(
                    {"obj_uid": str(i), "owner_uid": str(i), "value": value}
                )
            )

            assert result == expected

        for i, value in enumerate(obj_test_values["bad"]):
            with pytest.raises(TypeError):
                ObjectUpdate.from_json(
                    json.dumps({"uid": str(i), "value": value})
                )
