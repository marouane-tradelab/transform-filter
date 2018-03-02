#!/usr/bin/env python3

import argparse
import io
import sys
import json
import logging
import hashlib
import singer
import re
from oauth2client import tools

try:
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument('-c', '--config', help='Config file', required=True)
    flags = parser.parse_args()

except ImportError:
    flags = None

logger = singer.get_logger()

def test_record(record, filter_rules):
    for key, val in record.items():
        if key in filter_rules: 
            if isinstance(val, dict):
                if not test_record(record[key], filter_rules[key]): #call recursively
                    return False
            else:
                if key in filter_rules:
                    if not re.search(filter_rules[key], record[key]):
                        return False
    return True

def transform_lines(lines, filter_rules):
    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise
        if isinstance(msg, singer.RecordMessage):
            keep = test_record(msg.record, filter_rules)
            if keep:
                singer.write_records(msg.stream, [msg.record])
        elif isinstance(msg, singer.StateMessage):
            singer.write_state(msg.value)
        elif isinstance(msg, singer.SchemaMessage):
            singer.write_schema(msg.stream, msg.schema, msg.key_properties)
        else:
            raise Exception("Unrecognized message {}".format(msg))

def main():
    with open(flags.config) as input:
        config = json.load(input)
    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    transform_lines(input, config["filter_rules"])

if __name__ == '__main__':
    main()
