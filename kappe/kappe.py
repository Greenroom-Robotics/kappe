"""
Convert mcap files, e.g. from ROS1 to ROS2.

Message definitions:
    Message definitions are read from ROS and disk ./msgs/
    git clone --depth=1 --branch=humble https://github.com/ros2/common_interfaces.git msgs
"""

__version__ = '0.3.0'

import argparse
import logging
from multiprocessing import Pool, RLock, cpu_count
from pathlib import Path
from typing import Any

import yaml
from tqdm import tqdm

from kappe.convert import Converter
from kappe.settings import Settings


class TqdmLoggingHandler(logging.Handler):
    def emit(self, record: Any):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:  # noqa: BLE001
            self.handleError(record)


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S%z',
    handlers=[TqdmLoggingHandler()],
)


def worker(arg: tuple[Path, Path, Settings, int]):
    input_path, output_path, config, tqdm_idx = arg
    logging.info('Writing %s', output_path)
    try:
        conv = Converter(config, input_path, output_path)
        conv.process_file(tqdm_idx)
        conv.finish()
    except Exception:
        logging.exception('Failed to convert %s', input_path)
    logging.info('Done    %s', output_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        fromfile_prefix_chars='@',
    )
    parser.add_argument('input', type=str, help='input folder')
    parser.add_argument('output', type=str, help='output folder')
    parser.add_argument('--config', type=str, help='config file')
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Overwrite existing files')
    args = parser.parse_args()

    if args.config is None:
        config = Settings()
    else:
        with Path(args.config).open(encoding='utf-8') as f:
            config_text = f.read()
        config_yaml = yaml.safe_load(config_text)
        config = Settings(**config_yaml)
        config.raw_text = config_text

    # check for msgs folder
    if config.msg_folder is not None and not config.msg_folder.exists():
        logging.error('msg_folder does not exist: %s', config.msg_folder)
        config.msg_folder = None

    # check for plugins folder
    if config.plugin_folder is not None and not config.plugin_folder.exists() \
            and len(config.plugins) > 0:
        logging.error('plugin_folder does not exist: %s', config.plugin_folder)
        config.plugin_folder = None

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f'Input path does not exist: {input_path}')

    output_path = Path(args.output)

    tasks: list[tuple[Path, Path, Settings, int]] = []

    # TODO: make more generic
    if input_path.is_file():
        mcap_out = output_path / input_path.name
        if mcap_out.exists() and not args.overwrite:
            logging.info('File exists: %s -> skipping', mcap_out)
        else:
            tasks.append((input_path, mcap_out, config, 0))
    else:
        for idx, mcap_in in enumerate(input_path.rglob('**/*.mcap')):
            mcap_out = output_path / mcap_in.relative_to(input_path.parent)

            if mcap_out.exists() and not args.overwrite:
                logging.info('File exists: %s -> skipping', mcap_out)
                continue
            tasks.append((mcap_in, mcap_out, config, idx))

    thread_count = cpu_count()
    if config.general.threads is not None:
        thread_count = config.general.threads

    logging.info('Using %d threads', thread_count)
    tqdm.set_lock(RLock())  # for managing output contention

    pool = None
    try:
        pool = Pool(thread_count, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
        pool.map(worker, tasks)
    finally:
        if pool is not None:
            pool.terminate()
            pool.join()


if __name__ == '__main__':
    main()