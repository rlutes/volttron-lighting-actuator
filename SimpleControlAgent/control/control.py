import sys
import logging
from dataclasses import dataclass, field

from volttron.platform.agent import utils
from volttron.platform.vip.agent import Agent, Core
import gevent

utils.setup_logging()
_log = logging.getLogger(__name__)
__version__ = '1.0'

@dataclass
class Control:
    actuator: str
    device_list: list = field(default_factory=list)
    point_dict: dict = field(default_factory=dict)

class Release(Agent):
    def __init__(self, config_path, **kwargs):
        super(Release, self).__init__(**kwargs)
        config = utils.load_config(config_path)
        self.control_list = []
        for obj in config.get('control_list', {}):
            self.control_list.append(Control(**obj))

    @Core.receiver('onstart')
    def release(self, sender, **kwargs):
        for control in self.control_list:
            for device in control.device_list:
                for point, value in control.point_dict.items():
                    try:
                        result = self.vip.rpc.call(
                            control.actuator, 'set_point',
                            self.core.identity, device, value, point).get(timeout=4)
                    except gevent.Timeout as ex:
                        _log.warning(f"ERROR releasing {device} -- {point} -- with exception {ex}")
                        continue

def main(argv=sys.argv):
    """Main method called by the aip."""
    try:
        utils.vip_main(Release)
    except Exception as exception:
        _log.exception("unhandled exception")
        _log.error(repr(exception))


if __name__ == "__main__":
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
