import os
import sys
import logging
import gevent
from volttron.platform.agent import utils
from volttron.platform.messaging import topics, headers as headers_mod
from volttron.platform.vip.agent import Agent, Core, RPC
from volttron.platform.jsonrpc import RemoteError
from volttron.platform.agent.utils import setup_logging, parse_timestamp_string

__author__ = "Robert Lutes, robert.lutes@pnnl.gov"
__version__ = "1.0"
setup_logging()
_log = logging.getLogger(__name__)

class LightActuator(Agent):
    def __init__(self, config_path, **kwargs):
        super().__init__(**kwargs)
        self.device_list = utils.load_config(config_path)
        self.actuator = self.device_list.get("actuator", "platform.actuator")
        self.device_values = {}
        self.device_update = {}
        self.rpc_map = {}

    @Core.receiver("onstart")
    def starting_base(self, sender, **kwargs):
        """
        This function is a core receiver that is triggered on system startup. The method initializes device topics and RPC mappings for the devices in the device list.

        Parameters:
        - sender: The sender of the event, usually not used in this method.
        - **kwargs: Additional arguments passed to the receiver.

        Functionality:
        - Constructs base topics for device value updates and RPC calls using the topics.DEVICES_VALUE and topics.RPC_DEVICE_PATH utility methods.
        - Iterates over each device and its associated point list in the device_list.
        - For each device:
          - Constructs a specific device_topic using the base device topic and the device path.
          - Updates the rpc_map dictionary to map an RPC path to the device topic.
          - Initializes the object state for device updates with None in the device_update dictionary.
          - Creates a dictionary in device_values for the device, initializing all points to None.
          - Subscribes to the device topic on the pubsub system and associates it with the callback method new_data.
        """
        base_device_topic = topics.DEVICES_VALUE(campus="", building="", unit="", path=None, point="all")
        base_rpc_path = topics.RPC_DEVICE_PATH(campus="", building="", unit="", path=None, point=None)

        for device, point_list in self.device_list.items():
            device_topic = base_device_topic(path=device, point="")
            self.rpc_map[base_rpc_path(path=device)] = device_topic
            self.device_update[device_topic] = None
            self.device_values[device_topic] = {point: None for point in point_list}
            self.vip.pubsub.subscribe(peer="pubsub", prefix=device_topic, callback=self.new_data)

    def new_data(self, peer, sender, bus, topic, header, message):
        """
        Handles incoming data for a specific topic, updates device values, and records the update time.

        Parameters:
        peer: The peer identity of the entity sending the message.
        sender: The sender of the message.
        bus: The messaging bus through which the message was received.
        topic: The topic associated with the data being received.
        header: The metadata header of the message.
        message: A tuple containing the actual message data and its metadata.

        Logs the receipt of data for the provided topic. Updates the shared device values using the
        keys present in both the existing data and the incoming data. Records the latest update
        timestamp for the topic in device_update. Logs details of the updated device values.
        """
        _log.info(f"Received data for topic: {topic}")
        data, meta = message
        self.device_values[topic].update({k: data[k] for k in self.device_values[topic].keys() & data.keys()})
        self.device_update[topic] = parse_timestamp_string(header[headers_mod.TIMESTAMP])
        _log.debug(f"Updated device values for topic {topic}: {self.device_values[topic]}")

    @RPC.export
    def set_point(self, requester_id, topic, value, point=None, **kwargs):
        """
        set_point(requester_id, topic, value, point=None, **kwargs)

        Handles requests to set a specific point for a device, identified by the provided topic. The function parses the topic to extract the device path and point name, retrieves the associated points for the device, and attempts to set the value for each point.

        Parameters:
          requester_id: Identifier of the requester making the point set request.
          topic: The topic string in a format indicating the device and point (e.g., "device_path/point_name").
          value: The value to be set for the specified point(s).
          point: Optional argument, if provided, specifies a single point to target specifically within the device.
          **kwargs: Additional keyword arguments for extended functionality or options.

        Returns:
          A boolean indicating whether an error occurred during the operation:
            True - If an error occurred while setting any of the points.
            False - If all points were successfully set.

        Behavior:
          - Splits the provided topic to extract the device path and the point name.
          - Retrieves the defined points for the device path from the device list.
          - Logs a warning if no points are defined for the provided device path.
          - Iterates through the device's points, attempting to set the provided value for each.
          - Tracks errors and returns True if any point setting fails.

        Note:
          The function uses the provided requester's identity to ensure authorized access when setting points.
        """
        caller_identity = self.vip.rpc.context.vip_message.peer
        device_path, point_name = topic.rsplit('/', 1)
        device_points = self.device_list.get(device_path, [])
        _log.debug(f'Call set_point: {topic}, {value}, {point} -- device_points: {device_points}')
        if not device_points:
            _log.warning(f"No points defined for device path: {device_path}")
            return True  # Indicate an error

        errors_occurred = False

        for point in device_points:
            success = self._set_point(caller_identity, device_path, point, value)
            if not success:
                errors_occurred = True

        return errors_occurred

    @RPC.export
    def revert_point(self, requester_id, topic, point=None, **kwargs):
        """
        Handles the reversion of points for a given device and topic.

        This function reverts the specified point or all points associated with the given device path.
        It communicates with the device using the provided requester identity and schedules the operation
        via the context of an RPC call. The device path and point are determined through parsing the topic.

        Parameters:
            requester_id: Identifier of the entity making the request.
            topic: String used to locate the target device path and point.
            point: Optional; the specific point to be reverted. If None, all points for the device will be reverted.
            **kwargs: Additional keyword arguments passed for extensibility.

        Returns:
            A boolean flag indicating whether any errors occurred during the reversion.
            Returns True if errors occurred, otherwise False.

        Logs:
            Emits a warning log if no points are defined for the given device path.

        Notes:
            This function relies on the '_revert_point' helper method for individual point reversion.
        """
        caller_identity = self.vip.rpc.context.vip_message.peer
        device_path, point_name = topic.rsplit('/', 1)
        device_points = self.device_list.get(device_path, [])
        _log.debug(f'Call revert_point: {topic}, {point} -- device_points: {device_points}')
        if not device_points:
            _log.warning(f"No points defined for device path: {device_path}")
            return True  # Indicate an error

        errors_occurred = False

        for point in device_points:
            success = self._revert_point(caller_identity, device_path, point)
            if not success:
                errors_occurred = True

        return errors_occurred

    def _set_point(self, caller_identity, device_path, point, value):
        """
        Attempts to set a specified point on a device to a given value.

        Parameters:
        caller_identity: Identifier for the caller requesting the point to be set.
        device_path: The device path where the point is located.
        point: The specific point on the device to be set.
        value: The value to which the point should be set.

        Returns:
        True if the point was successfully set; otherwise, False.

        Raises:
        Handles RemoteError and gevent.Timeout exceptions, logging an error message if the process fails.
        """
        try:
            set_topic = topics.RPC_DEVICE_PATH(campus="", building="", unit="", path=device_path, point=point)
            _log.debug(f'Call set_point: {caller_identity}, {device_path}, {point} -- set_topic: {set_topic} -- value: {value}')
            result = self.vip.rpc.call(self.actuator, "set_point", caller_identity, set_topic, value).get(timeout=30)
            return True
        except (RemoteError, gevent.Timeout) as ex:
            _log.error(f"Failed to set {point} on {device_path}: {ex}")
            return False

    def _revert_point(self, caller_identity, device_path, point):
        """
        Reverts a point on a specified device path to its default state.

        Args:
            caller_identity (str): Identifier for the caller making the request.
            device_path (str): The path of the device where the point is located.
            point (str): The specific point on the device to revert.

        Returns:
            bool: True if the point was successfully reverted, False otherwise.

        Exceptions:
            Handles RemoteError or gevent.Timeout in case of communication issues with the actuator service.
        """
        try:
            set_topic = topics.RPC_DEVICE_PATH(campus="", building="", unit="", path=device_path, point=point)
            _log.debug(
                f'Call revert: {caller_identity}, {device_path}, {point} -- set_topic: {set_topic}')
            result = self.vip.rpc.call(self.actuator, "revert_point", caller_identity, set_topic).get(timeout=30)
            return True
        except (RemoteError, gevent.Timeout) as ex:
            _log.error(f"Failed to revert {point} on {device_path}: {ex}")
            return False

    @RPC.export
    def get_point(self, requester_id, topic, **kwargs):
        """
        This method retrieves the average value of a specific point under a given device path.

        Args:
            requester_id: Identifier for the requester, not directly used in the method.
            topic: The topic string containing the device path and point name, separated by a slash.
            **kwargs: Additional parameters, not directly used in the method.

        Returns:
            The average of all non-None point values in the given device path if such values exist, otherwise None.
        """
        device_path, point_name = topic.rsplit('/', 1)
        data = self.device_values.get(device_path, {}).values()
        data = [value for value in data if value is not None]
        _log.debug(f"Call get_point: {topic} -- data: {data}")
        if data:
            try:
                return sum(data) / len(data)
            except TypeError as ex:
                _log.error(f"Failed to calculate average value for {point_name} on {device_path}: {ex}")
        return None


def main(argv=sys.argv):
    """Main method called by the aip."""
    try:
        utils.vip_main(LightActuator)
    except Exception as exception:
        _log.exception("unhandled exception")
        _log.error(repr(exception))


if __name__ == "__main__":
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass