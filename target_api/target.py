"""Api target class."""

from __future__ import annotations

from typing import Type, Optional
import copy

from singer_sdk import Sink
from target_hotglue.target import TargetHotglue

from target_api.sinks import BatchSink, RecordSink
from singer_sdk.helpers._compat import final
from collections import OrderedDict
from target_hotglue.target_base import update_state


class TargetApi(TargetHotglue):
    """Sample target for Api."""

    name = "target-api"
    SINK_TYPES = [RecordSink, BatchSink]
    target_counter = {}

    @property
    def MAX_PARALLELISM(self):
        # If we want to process sequentially we cannot use parallelism
        # https://github.com/meltano/sdk/blob/main/singer_sdk/target_base.py#L521
        if self.config.get("enforce_order"):
            return 1

        return 10

    def __init__(
        self,
        config = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, parse_env_config, validate_config, state)

        # NOTE: We want to override this with an ordered dict to enforce order when we iterate later
        self._sinks_active = OrderedDict()

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        if self.config.get("process_as_batch", True):
            return BatchSink
        return RecordSink

    @final
    def drain_one(self, sink: Optional[Sink]) -> None:
        """Drain a specific sink.

        Args:
            sink: Sink to be drained.
        """
        # post empty records only if post_empty_record flag is set as True (it's False by default)
        if not self.config.get("post_empty_record", False):
            super().drain_one(sink)
        
        else:
            draining_status = sink.start_drain()
            # if there is schema but no records for a sink, post an empty record
            if not draining_status and (
                not sink.latest_state
                or not sink.latest_state.get("summary", {}).get(sink.name)
            ):
                draining_status = {"records": [{}]}
                sink.send_empty_record = True

            
            # send an empty record for batchSink
            if self.config.get("process_as_batch"):
                sink.process_batch(draining_status)
                sink.mark_drained()
            # send an empty record and update state for single record Sink
            else:
                sink.process_record({}, {})
                if not self._latest_state:
                    # If "self._latest_state" is empty, save the value of "sink.latest_state"
                    self._latest_state = sink.latest_state
                else:
                    for key in self._latest_state.keys():
                        sink_latest_state = sink.latest_state or dict()
                        self._latest_state[key].update(sink_latest_state.get(key) or dict())
                self._write_state_message(self._latest_state)
    
    @final
    def drain_all(self, is_endofpipe: bool = False) -> None:
        """Drains all sinks, starting with those cleared due to changed schema.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            is_endofpipe: This is passed by the
                          :meth:`~singer_sdk.Sink._process_endofpipe()` which
                          is called after the target instance has finished
                          listening to the stdin
        """
        state = copy.deepcopy(self._latest_state)
        self._drain_all(self._sinks_to_clear, 1)
        if is_endofpipe:
            for sink in self._sinks_to_clear:
                if sink:
                    sink.clean_up()
        self._sinks_to_clear = []
        self._drain_all(list(self._sinks_active.values()), self.max_parallelism)
        if is_endofpipe:
            for sink in self._sinks_active.values():
                if sink:
                    sink.clean_up()

        # Build state from BatchSinks
        batch_sinks = [s for s in self._sinks_active.values() if isinstance(s, BatchSink)]
        for s in batch_sinks:
            if s.name not in state.get("bookmarks", []):
                state = update_state(state, s.latest_state, self.logger)
            else:
                state["bookmarks"][s.name] = s.latest_state["bookmarks"][s.name]
                state["summary"][s.name] = s.latest_state["summary"][s.name]
        
        # for single record sinks drain_all is executed after processing the records therefore the latest_state is already populated
        # when there is no records drain_all is executed first so we process and write the state in drain_one and avoid writing an extra state here
        if self.config.get("post_empty_record", False) and not self.config.get("process_as_batch"):
            pass
        else:
            self._write_state_message(state)
            self._reset_max_record_age()

    def _process_record_message(self, message_dict: dict) -> None:
        """Process a RECORD message.

        Args:
            message_dict: TODO
        """
        self._assert_line_requires(message_dict, requires={"stream", "record"})

        stream_name = message_dict["stream"]
        for stream_map in self.mapper.stream_maps[stream_name]:
            # new_schema = helpers._float_to_decimal(new_schema)
            raw_record = copy.copy(message_dict["record"])
            transformed_record = stream_map.transform(raw_record)
            if transformed_record is None:
                # Record was filtered out by the map transform
                continue

            sink = self.get_sink(stream_map.stream_alias, record=transformed_record)
            context = sink._get_context(transformed_record)
            if sink.include_sdc_metadata_properties:
                sink._add_sdc_metadata_to_record(
                    transformed_record, message_dict, context
                )
            else:
                sink._remove_sdc_metadata_from_record(transformed_record)

            sink._validate_and_parse(transformed_record)

            sink.tally_record_read()
            transformed_record = sink.preprocess_record(transformed_record, context)
            sink.process_record(transformed_record, context)
            sink._after_process_record(context)

            if not self._latest_state:
                # If "self._latest_state" is empty, save the value of "sink.latest_state"
                self._latest_state = sink.latest_state
            else:
                # If "self._latest_state" is not empty, update all its fields with the
                # fields from "sink.latest_state" (if they exist)
                for key in self._latest_state.keys():
                    sink_latest_state = sink.latest_state or dict()
                    if isinstance(self._latest_state[key], dict):
                        self._latest_state[key].update(sink_latest_state.get(key) or dict())

if __name__ == "__main__":
    TargetApi.cli()
