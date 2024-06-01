"""Test functionality of stuff in shared.py."""

import uuid

import pytest

from doink.kafka import KafkaHandler
from doink.shared import SharedObject


class TestSharedObject:
    """Test functionality of the SharedObject."""

    @staticmethod
    @pytest.fixture
    def shared_object():
        """Create a child class of SharedObject for testing."""

        handler = KafkaHandler("topic")
        handler.start()

        uid = str(uuid.uuid4())

        class MySharedObject(SharedObject):
            """Test child of SharedObject."""

            def __init__(self, initial_shared_value=None):
                super().__init__(handler, "topic", uid)

                if getattr(self, "initial_shared_value", None) is None:
                    self.initial_shared_value = initial_shared_value

        yield handler, MySharedObject

        handler.stop()

    # pylint: disable=redefined-outer-name,unused-argument
    def test_shared_object_can_share_updates_no_existing_state(
        self, mock_kafka, shared_object
    ):
        """Test updates can be shared between two new objects."""

        handler, my_shared_object = shared_object

        obj1 = my_shared_object()
        obj2 = my_shared_object()

        obj1.my_cool_value = 123
        # pylint: disable=protected-access
        obj1._my_unshared_cool_value = 0.5

        # pylint: disable=protected-access
        obj2._my_unshared_cool_value = 1.5

        handler.stop()

        assert obj1.my_cool_value == 123
        # pylint: disable=protected-access
        assert obj1._my_unshared_cool_value == 0.5

        # pylint: disable=protected-access
        assert obj2._my_unshared_cool_value == 1.5

    # pylint: disable=redefined-outer-name,unused-argument
    def test_shared_object_can_share_updates_existing_state(
        self, mock_kafka, shared_object
    ):
        """Test updates can be shared with different object timelines."""

        handler, my_shared_object = shared_object

        obj1 = my_shared_object()
        obj1.my_cool_value = "something_cool"
        obj2 = my_shared_object()

        handler.stop()

        assert obj2.my_cool_value == "something_cool"

    def test_shared_object_can_share_state_set_in_init(
        self, mock_kafka, shared_object
    ):
        """Test shared objects can set attributes in __init__."""

        handler, my_shared_object = shared_object

        obj1 = my_shared_object("an_initial_value")
        obj2 = my_shared_object("an_initial_value")

        obj1.initial_shared_value = "a_new_value"

        handler.stop()

        assert obj1.initial_shared_value == "a_new_value"
        assert obj2.initial_shared_value == "a_new_value"

    def test_shared_object_can_share_state_set_in_init_existing(
        self, mock_kafka, shared_object
    ):
        """Test attributes set in __init__ work with different timelines."""

        handler, my_shared_object = shared_object

        obj1 = my_shared_object("an_initial_value")
        obj1.initial_shared_value = "a_new_value"

        # Wait for messages to propagate
        handler.stop()

        # Should pull from kafka_handlers internal state.
        obj2 = my_shared_object("an_initial_value")

        handler.stop()

        assert obj1.initial_shared_value == "a_new_value"
        assert obj2.initial_shared_value == "a_new_value"
