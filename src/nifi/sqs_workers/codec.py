import base64
import io
from typing import List

from nifi.flowfile import FlowFile
from sqs_workers.codecs import CONTENT_TYPES_CODECS, get_codec
from nifi.flowfile.stream import FlowFileStreamReader, FlowFileStreamWriter

__all__ = ["get_codec"]


class FlowFileStreamCodec(object):
    @staticmethod
    def serialize(flowfiles: List[FlowFile]):
        with io.BytesIO() as bytes_out:
            writer = FlowFileStreamWriter(bytes_out)
            writer.write_all(flowfiles)
            ff3_data = bytes_out.getvalue()
        return base64.b64encode(ff3_data).decode("utf-8")

    @staticmethod
    def deserialize(serialized) -> List[FlowFile]:
        ff3_data = base64.b64decode(serialized.encode("utf-8"))
        with io.BytesIO(ff3_data) as bytes_in:
            reader = FlowFileStreamReader(bytes_in)
            messages = [FlowFile(attributes, content) for attributes, content in reader]
        return messages


CONTENT_TYPES_CODECS["flowfile-v3"] = FlowFileStreamCodec
