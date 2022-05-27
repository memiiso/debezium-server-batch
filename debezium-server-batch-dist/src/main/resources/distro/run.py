import argparse
import jnius_config
import logging
import sys
#####  loggger
from pathlib import Path

log = logging.getLogger(name="debezium")
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s  [%(module)s] (%(funcName)s) %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)


#####

class Debezium():

    def __init__(self, debezium_dir: str = None, conf_dir: str = None):
        if debezium_dir is None:
            self.debezium_server_dir: Path = Path(__file__).resolve().parent
        else:
            if not Path(debezium_dir).is_dir():
                raise Exception("Debezium Server directory '%s' not found" % debezium_dir)
            self.debezium_server_dir: Path = Path(debezium_dir)
            log.info("Setting Debezium dir to:%s" % self.debezium_server_dir.as_posix())

        if conf_dir is None:
            self.conf_dir = self.debezium_server_dir.joinpath("conf")
        else:
            if not Path(conf_dir).is_dir():
                raise Exception("Debezium conf directory '%s' not found" % conf_dir)
            self.conf_dir: Path = Path(conf_dir)
            log.info("Setting conf dir to:%s" % self.conf_dir.as_posix())

        ##### jnius
        DEBEZIUM_CLASSPATH: list = [
            self.debezium_server_dir.joinpath('*').as_posix(),
            self.debezium_server_dir.joinpath("lib/*").as_posix(),
            self.conf_dir.as_posix()]

        if not jnius_config.vm_running:
            jnius_config.add_classpath(*DEBEZIUM_CLASSPATH)
            log.info("VM Classpath: %s" % jnius_config.get_classpath())
        else:
            log.warning("VM is already running, can't set classpath/options")
            log.debug("VM started at %s" % jnius_config.vm_started_at)

    def run(self, *args: str):

        jnius_config.add_options(*args)
        log.info("Configured jvm options:%s" % jnius_config.get_options())

        from jnius import autoclass
        DebeziumServer = autoclass('io.debezium.server.Main')
        _dbz = DebeziumServer()
        return _dbz.main()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debezium_dir', type=str, default=None,
                        help='Directory of debezium server application')
    parser.add_argument('--conf_dir', type=str, default=None,
                        help='Directory of application.properties')
    _args, args = parser.parse_known_args()
    ds = Debezium(debezium_dir=_args.debezium_dir, conf_dir=_args.conf_dir)
    ds.run(*args)


if __name__ == '__main__':
    main()
