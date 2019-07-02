import random
import os
import logging
import sys
import time

logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
handler = logging.FileHandler("log.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def get_pool():
    f = open("pool/pool.txt","r")
    lines = f.read().split("\n")
    f.close()
    return lines

def select_one(pool):
    return random.sample(pool,1)[0]

def create_yaml(name,job,scheduler):
    f_pool = open("pool/"+name+".yaml","r")
    conf = f_pool.read()
    conf = conf.replace("Needtoplaced",job+"-"+name)
    conf = conf.replace("scheduler-need-to-be-replaced",scheduler)
    f_pool.close()
    f_output = open(job+".yaml","w")
    f_output.write(conf)
    f_output.close()


def create_scheduler():
    logger.info("create a new scheduler")
    f = open("scheduler.csv","w")
    f.write("yaml,second,name\n")
    f.close()

def add_scheduler(name,job,time):
    second = random.randint(0,time)
    with open("scheduler.csv","a") as f:
        f.write(job+".yaml,"+str(second)+","+job+"-"+name+"\n")
        logger.info("add "+job+"-"+name+",submitted on "+str(second))
        f.close()

def main(job_num,time,scheduler):
    os.popen("rm *.yaml")
    logger.info("delete all previous yamls")
    create_scheduler()
    pool = get_pool()
    for i in range(1,job_num+1):
        job = "job"+str(i)
        name = select_one(pool)
        create_yaml(name,job,scheduler)
        logger.info("create a "+name+" yaml named "+job+".yaml")
        add_scheduler(name,job,time)
    logger.info("successful created yamls and scheduler")


if __name__ == "__main__":
    job_num = int(sys.argv[1])
    time = int(sys.argv[2])
    scheduler = sys.argv[3]
    main(job_num,time,scheduler)

