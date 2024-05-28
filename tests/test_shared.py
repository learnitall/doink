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

            def __init__(self):
                super().__init__(handler, "topic", uid)
                self.my_cool_value = "my_cool_value"
                self.my_other_cool_value = "my_other_cool_value"
                self._my_unshared_cool_value = "my_unshared_cool_value"

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
        obj1.my_other_cool_value = True
        obj1._my_unshared_cool_value = None  # pylint: disable=protected-access

        handler.stop()

        assert obj2.my_cool_value == obj1.my_cool_value
        assert obj2.my_other_cool_value == obj1.my_other_cool_value
        # pylint: disable=protected-access
        assert obj2._my_unshared_cool_value != obj1._my_unshared_cool_value

    # pylint: disable=redefined-outer-name,unused-argument
    @pytest.mark.skip
    def test_shared_object_can_share_updates_existing_state(
        self, mock_kafka, shared_object
    ):
        """
        Test updates can be shared with different object timelines.

        This fails always, because there currently is no way to
        pull state from Kafka when an object is initialized - each
        object assumes it is the first object created and pushes
        a full update.

        When obj2 is created below, it pushes an update containing
        the default value for 'my_cool_value', which obj1 then
        picks up.
        """

        handler, my_shared_object = shared_object

        obj1 = my_shared_object()
        obj1.my_cool_value = "my_cool_value_changed"

        obj2 = my_shared_object()

        handler.stop()

        assert obj2.my_cool_value == "my_cool_value_changed"
        assert obj1.my_cool_value == "my_cool_value_changed"
