from typing import Dict, Set, Tuple

from scaler.protocol.python.message import (
    ObjectContent,
    ObjectInstruction,
    ObjectInstructionType,
    ObjectRequest,
    ObjectResponse,
    ObjectResponseType,
)
from scaler.worker.agent.mixins import ObjectTracker
from scaler.utility.many_to_many_dict import ManyToManyDict


class VanillaObjectTracker(ObjectTracker):
    def __init__(self):
        self._object_request_to_processor_ids: ManyToManyDict[Tuple[bytes, ...], bytes] = ManyToManyDict()
        self._object_id_to_processors_ids: ManyToManyDict[bytes, bytes] = ManyToManyDict()

    def on_object_request(self, processor_id: bytes, object_request: ObjectRequest) -> None:
        self._object_request_to_processor_ids.add(object_request.object_ids, processor_id)

    def on_object_response(self, object_response: ObjectResponse) -> Set[bytes]:
        """Returns a list of processor ids that requested this object content."""

        if object_response.type != ObjectResponseType.Content:
            raise TypeError(f"invalid object response type received: {object_response.type}.")

        object_ids = object_response.object_content.object_ids

        if not self._object_request_to_processor_ids.has_left_key(object_ids):
            return set()

        processor_ids = self._object_request_to_processor_ids.remove_left_key(object_ids)

        for processor_id in processor_ids:
            for object_id in object_ids:
                self._object_id_to_processors_ids.add(object_id, processor_id)

        return processor_ids

    def on_object_instruction(self, object_instruction: ObjectInstruction) -> Dict[bytes, ObjectInstruction]:
        """
        From an object instruction received by the worker, returns the sub-object instructions that should be
        forwarded to processors.
        """

        if object_instruction.type != ObjectInstructionType.Delete:
            raise TypeError(f"invalid object instruction type received: {object_instruction.type}.")

        per_processor_object_ids = {}
        for object_id in object_instruction.object_content.object_ids:
            if not self._object_id_to_processors_ids.has_left_key(object_id):
                continue

            processor_ids = self._object_id_to_processors_ids.remove_left_key(object_id)
            for processor_id in processor_ids:
                if processor_id not in per_processor_object_ids:
                    per_processor_object_ids[processor_id] = []

                per_processor_object_ids[processor_id].append(object_id)

        return {
            processor_id: ObjectInstruction(
                type=ObjectInstructionType.Delete,
                object_user=object_instruction.object_user,
                object_content=ObjectContent(object_ids=object_ids),
            )
            for processor_id, object_ids in per_processor_object_ids.items()
        }

    def on_processor_end(self, processor_id: bytes) -> None:
        if self._object_request_to_processor_ids.has_right_key(processor_id):
            self._object_request_to_processor_ids.remove_right_key(processor_id)

        if self._object_id_to_processors_ids.has_right_key(processor_id):
            self._object_id_to_processors_ids.remove_right_key(processor_id)
