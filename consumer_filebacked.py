#!/usr/bin/env python
import json
import csv
import argparse
import asyncio
from os import path
import esb
async def __main__(args):
    q = esb.Queue(timeout=10,path="queues/q1",name="q1")
    while True:
        item = await q.next()
        if item:
            print(await item.get())
            await item.ack()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Consume messages')
    asyncio.run(__main__(parser.parse_args()))
