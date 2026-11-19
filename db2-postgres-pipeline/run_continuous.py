#!/usr/bin/env python3
"""
Continuous Pipeline Runner - starts a single named pipeline and keeps
re-running its incremental check on an interval, until it errors out or
is stopped (SIGTERM/SIGINT/Ctrl+C). Used by the admin dashboard so that
clicking "Run" on a pipeline keeps it running until the admin stops it
or it fails, instead of running once and exiting.
"""

import sys
import os
import time
import signal
import inspect
import logging
import argparse

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from run_all_pipelines import PipelineOrchestrator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

_stop_requested = False


def _request_stop(signum, frame):
    global _stop_requested
    logger.info(f"Stop signal ({signum}) received, finishing current cycle then shutting down...")
    _stop_requested = True


signal.signal(signal.SIGTERM, _request_stop)
signal.signal(signal.SIGINT, _request_stop)


def resolve_pipeline_class(pipeline_name):
    config = next((p for p in PipelineOrchestrator.PIPELINES if p['name'] == pipeline_name), None)
    if config is None:
        raise ValueError(f"Unknown pipeline: {pipeline_name}")

    module_path = f"{config['module']}.{config['module']}_streaming_pipeline"
    module = __import__(module_path, fromlist=[config['class']])
    return getattr(module, config['class'])


def run_cycle(pipeline_class, full_load):
    pipeline = pipeline_class()
    run_method = pipeline.run_streaming_pipeline

    # Not every pipeline has been migrated to incremental mode yet - call
    # with 'incremental' only if the method actually accepts it.
    if 'incremental' in inspect.signature(run_method).parameters:
        run_method(incremental=not full_load)
    else:
        run_method()


def main():
    parser = argparse.ArgumentParser(description='Continuous Pipeline Runner')
    parser.add_argument('pipeline_name', help='Pipeline name, as listed in run_all_pipelines.py')
    parser.add_argument('--full-load', action='store_true', help='Run every cycle as a full load instead of incremental')
    parser.add_argument('--interval-seconds', type=int, default=60, help='Delay between cycles once a cycle completes')
    args = parser.parse_args()

    pipeline_class = resolve_pipeline_class(args.pipeline_name)

    logger.info(f"Starting continuous run for '{args.pipeline_name}' (interval={args.interval_seconds}s, full_load={args.full_load})")

    while not _stop_requested:
        try:
            run_cycle(pipeline_class, args.full_load)
        except Exception as e:
            logger.error(f"Pipeline '{args.pipeline_name}' errored, stopping continuous run: {e}")
            sys.exit(1)

        if _stop_requested:
            break

        for _ in range(args.interval_seconds):
            if _stop_requested:
                break
            time.sleep(1)

    logger.info(f"Continuous run for '{args.pipeline_name}' stopped.")


if __name__ == "__main__":
    main()
