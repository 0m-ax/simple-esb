#!/usr/bin/env python
import json
import csv
import argparse
import asyncio
import esb
async def __main__(args):
    q = esb.QueueClient('q1')
    while True:
        item = await q.next()
        if item:
            print(await item.get())
            await item.ack()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Consume messages')
    asyncio.run(__main__(parser.parse_args()))
