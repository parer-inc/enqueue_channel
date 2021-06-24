"""This service allows to enqueu channels to parse"""
import os
import sys
import time
from rq import Worker, Queue, Connection
from methods.connection import get_redis, await_job

r = get_redis()


def enqueue_channel(chan):
    """Enqueues channel to parse"""
    result = False
    for i in range(4):
        q = Queue('get_channels', connection=r)
        job = q.enqueue('get_channels.get_channels',
                        "WHERE", "id", chan[1])
        await_job(job)
        result = job.result
        if result is not False:
            break
        time.sleep(5)
    chan_type = "upd" if result != () else "new"

    q = Queue('parse_channel', connection=r, default_timeout=18000)
    job = q.enqueue('parse_channel.parse_channel', chan[1])
    await_job(job, 18000)
    result = job.result
    print("Done parsing in enqeueu", result)
    if result:
        if chan_type == "new":
            print("wrote new!")
            q = Queue('write_channels', connection=r)
            job = q.enqueue('write_channels.write_channels', [result])
        else:
            q = Queue('update_channels', connection=r)
            job = q.enqueue('update_channels.update_channels', [result])
    else:
        # LOG
        return False

    q = Queue('get_tmp_table', connection=r)
    job = q.enqueue('get_tmp_table.get_tmp_table', chan[1]+"_tmp", "data")
    await_job(job)
    vids_to_parse = job.result
    if vids_to_parse != () and vids_to_parse:
        for vid in vids_to_parse:
            q = Queue('enqueue_video', connection=r)
            q.enqueue('enqueue_video.enqueue_video', vid[0], chan[1])
    else:
        return False
    # q = Queue('delete_tmp_table', connection=r)
    # job = q.enqueue('delete_tmp_table.delete_tmp_table', chan[1]+"_tmp")
    q = Queue('delete_task', connection=r)
    job = q.enqueue('delete_task.delete_task', chan[0])
    return True


if __name__ == '__main__':
    q = Queue('enqueue_channel', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r,  name='enqueue_channel')
        worker.work()
