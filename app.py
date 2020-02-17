from flask import Flask, request
import redis
from rq import Connection, Worker, Queue
import time
import multiprocessing

r = redis.Redis()
app = Flask(__name__)

def list_workers():
    def serialize_queue_names(worker):
        return [q.name for q in worker.queues]

    workers = [dict(name=worker.name, queues=serialize_queue_names(worker),
        state=worker.get_state()) for worker in Worker.all()]
    return dict(workers=workers)

def runworker(queuename):
    #Burst mode kills the worker once it has completed its jobs
    qs=[queuename]
    with Connection(r):
        if (get_workers().split(';')[0] == "0") :
            time.sleep(5)
            multiprocessing.Process(target=Worker(qs,connection=r,name=queuename).work, kwargs={'burst': False}).start()
        if Worker.find_by_key('rq:worker:'+queuename):
            workerInstance = Worker.find_by_key('rq:worker:name')
            print("Worker for queue "+queuename+" is already running")
            print("Current amount of running jobs: ")
        else:
            try:
                print("worker was not alive, starting")
                multiprocessing.Process(target=Worker(qs,connection=r,name=queuename).work, kwargs={'burst': False}).start()
                #worker = Worker(qs, connection=r, name=queuename)
                # print("Workers: "+str(list_workers()))
                #print("worker pid: "+multiprocessingstr(.Process(target=Worker)(qs).work).pid())
                if (get_workers() == "0") :
                    time.sleep(5)
                    multiprocessing.Process(target=Worker(qs,connection=r,name=queuename).work, kwargs={'burst': False}).start()
            except:
                print("Falhou o start do worker")
                pass

def background_task(n):

    delay = 5

    print("Task running")
    print(f"Simulating {delay} second delay")
    time.sleep(delay)
    print("Task complete")
    return len(n)


@app.route("/workers")
def get_workers():
    queuename = request.args.get("qname")
    # Count the number of workers in this Redis connection
    workersRedisCount = Worker.count(connection=r)
    print("Current number of workers in this Redis: "+ str(workersRedisCount))

    # Count the number of workers for a specific queue
    queue = Queue(queuename, connection=r)
    q_len = len(queue)
    workersQueueInstance = Worker.all(queue=queue)
    print("Current number of started workers for "+queuename+" equal to "+str(workersQueueInstance))
    return str(workersRedisCount)+";"+str(q_len)

@app.route("/task")
def add_task():
    
    if request.args.get("n"):
        runworker(request.args.get("qname"))
        q = Queue(request.args.get("qname"),connection=r)
        job = q.enqueue(background_task, request.args.get("n"))
        
        q_len = len(q)

        return f"Task {job.id} added to queue at {job.enqueued_at}. {q_len} tasks in the queue \n args: {job.args}"
    
    return "No value for n"

if __name__ == "__main__":
    app.run()
