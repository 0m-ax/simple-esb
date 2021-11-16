from aiohttp import web
import os, os.path
import time
import aiohttp
import pickle
class QueueClient:
    def __init__(self,queue,server="http://localhost:8080/"):
        self.url = server+"queue/"+queue
        self._session =  aiohttp.ClientSession()
    async def next_item(self):
        resp = await self._session.get(self.url+"/next")
        if resp.status == 200:
            return await resp.text()
        return None
    async def next(self):
        item = await self.next_item()
        if item is not None:
            return QueueItem(self,item)
        return None
    async def ack_item(self,item_id):
        resp = await self._session.get(self.url+"/item/"+item_id+"/ack")
        if resp.status == 200:
            return True
        return False
    async def nack_item(self,item_id):
        resp = await self._session.get(self.url+"/item/"+item_id+"/nack")
        if resp.status == 200:
            return True
        return False
    async def get_item(self,item_id):
        resp = await self._session.get(self.url+"/item/"+item_id)
        return await resp.read()
    async def push_item(self,body):
        resp = await self._session.post(self.url+"/push",data=body)

class QueueItem:
    def __init__(self,queue,item_id):
        self._queue = queue
        self.item_id = item_id
    def ack(self):
       return self._queue.ack_item(self.item_id)
    def nack(self):
       return self._queue.nack_item(self.item_id)
    def get(self):
       return self._queue.get_item(self.item_id)

class Queue:
    def __init__(self,path,name,timeout=30,input_transformer=lambda x:x,output_transformer=lambda x:x):
        self.name = name
        self.timeout=timeout
        self.path = path
        self.input_transformer = input_transformer
        self.output_transformer = output_transformer

        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        self.path_items = os.path.join(self.path,'items')
        if not os.path.isdir(self.path_items):
            os.mkdir(self.path_items)
        self.path_tmp = os.path.join(self.path,'tmp')
        if not os.path.isdir(self.path_tmp):
            os.mkdir(self.path_tmp)
        self.path_metadata = os.path.join(self.path,'metadata')
        if not os.path.isdir(self.path_metadata):
            os.mkdir(self.path_metadata)
        self.path_metadata_time_opened = os.path.join(self.path_metadata,'time_opened')
        if not os.path.isdir(self.path_metadata_time_opened):
            os.mkdir(self.path_metadata_time_opened)
        self.path_last_id = os.path.join(self.path,'last_id')
        if not os.path.exists(self.path_last_id):
            with open(self.path_last_id,'w') as fp:
                fp.write(str(0))

    async def push_item(self,body):
        id=None
        with open(self.path_last_id,'r+') as fp:
            id = fp.read()
            fp.seek(0)
            fp.write(str(int(id)+1))
        path_tmp = os.path.join(self.path_tmp,id)
        with open(path_tmp,'wb') as fp:
            fp.write(pickle.dumps(self.input_transformer(body)))
        path_item = os.path.join(self.path_items,id)
        os.rename(path_tmp,path_item)

    async def get_item(self,item_id):
        path_item = os.path.join(self.path_items,item_id)
        if os.path.exists(path_item):
            with open(path_item,'rb') as fp:
                return self.output_transformer(pickle.loads(fp.read()))

    async def next_item(self):
        items = os.listdir(self.path_items)
        items.sort()
        for item_id in items:
            path_item_time_opened = os.path.join(self.path_metadata_time_opened,item_id)
            if os.path.exists(path_item_time_opened):
                with open(path_item_time_opened,'r+') as fp:
                    if float(fp.read())+self.timeout < time.time():
                        fp.seek(0)
                        fp.write(str(time.time()))
                        return item_id
            else:
                with open(path_item_time_opened,'w') as fp:
                    fp.write(str(time.time()))
                    return item_id
        return None
    async def next(self):
        item = await self.next_item()
        if item is not None:
            return QueueItem(self,item)
        return None

    async def ack_item(self,item_id):
        path_item_time_opened = os.path.join(self.path_metadata_time_opened,item_id)
        path_item = os.path.join(self.path_items,item_id)
        if os.path.exists(path_item):
            os.remove(path_item)
        if os.path.exists(path_item_time_opened):
            os.remove(path_item_time_opened)

    async def nack_item(self,item_id):
        path_item_time_opened = os.path.join(self.path_metadata_time_opened,item_id)
        if os.path.exists(path_item_time_opened):
            os.remove(path_item_time_opened)



class QueueHTTPServer:
    async def get_home(self,request):
        return web.Response(text="Hello!")

    async def post_queue_push(self,request):
        queue_id = request.match_info.get('queue')
        await self.queues_manager.queues[queue_id].push_item(await request.read())
        return web.Response()

    async def get_queue_next(self,request):
        queue_id = request.match_info.get('queue')
        next_item = await self.queues_manager.queues[queue_id].next_item()
        if next_item is not None:
            return web.Response(text=next_item)
        return web.Response(status=204)

    async def get_queue_item(self,request):
        queue_id = request.match_info.get('queue')
        item_id = request.match_info.get('item')
        data = await self.queues_manager.queues[queue_id].get_item(item_id)
        resp = web.StreamResponse()
        await resp.prepare(request)
        await resp.write(data)
        return resp

    async def get_queue_item_ack(self,request):
        queue_id = request.match_info.get('queue')
        item_id = request.match_info.get('item')

        await self.queues_manager.queues[queue_id].ack_item(item_id)
        return web.Response()

    async def get_queue_item_nack(self,request):
        queue_id = request.match_info.get('queue')
        item_id = request.match_info.get('item')

        await self.queues_manager.queues[queue_id].nack_item(item_id)
        return web.Response()

    app = web.Application()
    def __init__(self,queues_manager=[]):
        self.queues_manager = queues_manager
        self.app.add_routes([web.get('/', self.get_home),
            web.post('/queue/{queue}/push', self.post_queue_push),
            web.get('/queue/{queue}/next', self.get_queue_next),
            web.get('/queue/{queue}/item/{item}/ack', self.get_queue_item_ack),
            web.get('/queue/{queue}/item/{item}/nack', self.get_queue_item_nack),
            web.get('/queue/{queue}/item/{item}', self.get_queue_item)
        ])
    def run(self):
        web.run_app(self.app)    


class QueuesManager:
    def __init__(self,queue_configs=[]):
        self.queues = {}
        for queue_config in queue_configs:
            queue = Queue(**queue_config)
            self.queues[queue.name] = queue
