#!/usr/bin/env python
import json
import csv
import argparse
import asyncio
import esb
async def __main__(args):
    reader = csv.DictReader(args.file)
    q = esb.QueueClient('q1')
    for row in reader:
        await q.push_item(json.dumps(row))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Publish messages from csv')
    parser.add_argument('file', type=argparse.FileType('r'),
                        help='input file')
    asyncio.run(__main__(parser.parse_args()))
