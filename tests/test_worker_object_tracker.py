import unittest

from scaler.protocol.python.message import (
    ObjectContent,
    ObjectInstruction,
    ObjectInstructionType,
    ObjectRequest,
    ObjectRequestType,
    ObjectResponse,
    ObjectResponseType,
)
from scaler.worker.agent.object_tracker import VanillaObjectTracker


class TestWorkerObjectTracker(unittest.TestCase):
    def test_object_tracker(self) -> None:
        tracker = VanillaObjectTracker()

        tracker.on_object_request(b"processor_1", ObjectRequest(ObjectRequestType.Get, (b"object_1", b"object_2")))

        tracker.on_object_request(b"processor_2", ObjectRequest(ObjectRequestType.Get, (b"object_1", b"object_2")))
        tracker.on_object_request(b"processor_2", ObjectRequest(ObjectRequestType.Get, (b"object_3",)))

        tracker.on_object_request(b"processor_3", ObjectRequest(ObjectRequestType.Get, (b"object_4", b"object_5")))

        response_1 = tracker.on_object_response(
            ObjectResponse(ObjectResponseType.Content, ObjectContent((b"object_1", b"object_2")))
        )
        self.assertSetEqual(set(response_1), {b"processor_1", b"processor_2"})

        response_2 = tracker.on_object_response(
            ObjectResponse(ObjectResponseType.Content, ObjectContent((b"object_unknown",)))
        )
        self.assertSetEqual(response_2, set())

        response_3 = tracker.on_object_response(
            ObjectResponse(ObjectResponseType.Content, ObjectContent((b"object_3",)))
        )
        self.assertSetEqual(response_3, {b"processor_2"})

        object_instructions = tracker.on_object_instruction(
            ObjectInstruction(
                ObjectInstructionType.Delete,
                b"client",
                ObjectContent(
                    (b"object_1", b"object_2", b"object_3"),
                    (b"name_1", b"name_2", b"name_3"),
                    (b"content_1", b"content_2", b"content_3"),
                ),
            )
        )
        self.assertSetEqual(set(object_instructions.keys()), {b"processor_1", b"processor_2"})

        tracker.on_processor_end(b"processor_3")

        self.assertEqual(len(tracker._object_request_to_processor_ids.left_keys()), 0)
        self.assertEqual(len(tracker._object_id_to_processors_ids.left_keys()), 0)