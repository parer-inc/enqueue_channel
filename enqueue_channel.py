"""This service allows to write new channels to db"""
import os
import sys
import time
from rq import Worker, Queue, Connection
from methods.connection import get_redis, await_job


def enqueue_channel(chan):
    """Enqueues channel to parse"""
    result = False
    for i in range(4):
        q = Queue('get_channels', connection=r)
        job = q.enqueue('get_channels.get_channels',
                        "WHERE", "id", chan[1])
        await_job(job)
        result = job.result
        if result:
            break
        time.sleep(5)
    chan_type = "upd" if result != () else "new"

    q = Queue('parse_channel', connection=r, default_timeout=18000)
    job = q.enqueue('parse_channel.parse_channel', chan[1])
    await_job(job, 18000)
    result = job.result
    if result:
        if chan_type == "new":
            q = Queue('write_channels', connection=r)
            job = q.enqueue('write_channels.write_channels', result)
        else:
            # MB ZROBITI CHEREZ DICTIONARY? TYPU JSON
            q = Queue('update_channels', connection=r)
            job = q.enqueue('update_channels.update_channels', result)
    else:
        # LOG
        return False

    q = Queue('get_tmp_table', connection=r)
    job = q.enqueue('get_tmp_table.get_tmp_table', chan[1]+"_tmp", "*")
    vids_to_parse = job.result
    if vids_to_parse != () and vids_to_parse:
        for vid in vids_to_parse:
            print("+job", vid)
            q = Queue('enqueue_video', connection=r)
            job = q.enqueue('enqueue_video.enqueue_video', vid[0])
    else:
        return False
    q = Queue('delete_tmp_table', connection=r)
    job = q.enqueue('delete_tmp_table.delete_tmp_table', chan[1]+"_tmp")
    q = Queue('delete_task', connection=r)
    job = q.enqueue('delete_task.delete_task', chan[0])


if __name__ == '__main__':
    time.sleep(5)
    r = get_redis()
    q = Queue('enqueue_channel', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r,  name='enqueue_channel')
        worker.work()
