import pandas as pd
import time
import re
import os
from threading import Thread



class job_list(object):
    def __init__(self,joblist):
        self.jobs = pd.read_csv(joblist)
        self.stop = self.jobs.seconds.max()
        self.job_plans = [i for i in self.jobs["name"]]
        self.monitor_job = self.job_plans
        self.job_dict = {}
        self.run_job_list()
    # run job list start at a specific time
    def run_job_list(self):
        self.init_job_dict()
        for i in range((self.stop)+1):
            jobs_i = self.jobs[self.jobs.seconds == i]
            if jobs_i.shape[0] > 0:
                for index, job_i in jobs_i.iterrows():
                    yaml = job_i['yaml']
                    os.popen('kubectl '+'apply '+'-f '+yaml)
            self.renew_job_dict(self.job_dict,self.job_plans)
            time.sleep(1)
    # initialize job_dict : add all current nodes into this dict
    def init_job_dict(self):
        job_dict = {}
        node_info = os.popen("kubectl get nodes").read().split("\n")
        for i in range(1,len(node_info)-1):
            line = node_info[i]
            line = " ".join(line.split())
            match = re.match(r"(.*) (.*) (.*) (.*) (.*)",line)
            node = match.group(1)
            job_dict[node] = []
            self.job_dict = job_dict
    # renew_job_dict : add value of pods with job_list
    def renew_job_dict(self,current_job_dict,job_plans):
        pair_info = os.popen("kubectl get pod -o=custom-columns=NODE:.spec.nodeName,NAME:.metadata.name --all-namespaces").read().split("\n")
        for i in range(1,len(pair_info)-1):
            line = pair_info[i]
            line = " ".join(line.split())
            match = re.match(r"(.*) (.*)", line)
            node = match.group(1)
            pod = match.group(2)
            if (node in current_job_dict) and (pod in job_plans):
                if pod not in current_job_dict[node]:
                    current_job_dict[node].append(pod)
    def get_job_dict(self):
        return self.job_dict


def init_job_time(scheduler):
    jobs = pd.read_csv(scheduler)
    job_time = [i for i in jobs["seconds"]]
    job_name = [i for i in jobs["name"]]
    job_time_dict = {}
    for i in range(len(job_time)):
        job_time_dict[job_name[i]] = [job_time[i]]
    monitor_list = job_name
    return job_time_dict,monitor_list

def check_complete(job_time_dict,monitor_list,init_time):
    while True:
        pod_info = os.popen("kubectl get pods").read().split("\n")
        if len(monitor_list) ==0:
            return job_time_dict
        for i in range(1,len(pod_info)-1):
            line = pod_info[i]
            line = " ".join(line.split())
            match = re.match(r"(.*) (.*) (.*) (.*) (.*)", line)
            pod = match.group(1)
            status = match.group(3)
            if pod in monitor_list and status == "Completed":
                job_time_dict[pod].append(float(time.time()-init_time))
                monitor_list.remove(pod)



class run_job(Thread):
    def __init__(self, job_list):
        Thread.__init__(self)
        self.job_list = job_list

    def run(self):
        self.job_list.run_job_list()
    def get_result(self):
        return self.job_list.get_job_dict()

class record_time(Thread):
    def __init__(self,init_time):
        Thread.__init__(self)
        self.result = {}
        self.init_time = init_time
    def run(self):
        job_time_dict, monitor_list = init_job_time("scheduler.csv")
        self.result = check_complete(job_time_dict,monitor_list,self.init_time)

    def get_result(self):
        return self.result










if __name__ == "__main__":
    init_time = time.time()
    jobs = job_list("scheduler.csv")
    thread_1 = run_job(jobs)
    thread_2 = record_time(init_time)
    thread_1.start()
    thread_2.start()
    thread_1.join()
    thread_2.join()
    node_pod_dict = thread_1.get_result()
    pod_time_dict = thread_2.get_result()
    print("node,job,start_time,end_time,complete_time")
    for node in node_pod_dict:
        if len(node_pod_dict[node]) != 0:
            for job in node_pod_dict[node]:
                node_name = node.replace("."," ")
                match = re.match(r"(.*) (.*) (.*) (.*) (.*) (.*)", node_name)
                node_name = match.group(1)
                print("{},{},{},{},{}".format(node_name,job,str(pod_time_dict[job][0]),
    str(pod_time_dict[job][1]),str(pod_time_dict[job][1]-pod_time_dict[job][0])))






