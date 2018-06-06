import sys
import json
import random
from datetime import datetime, timedelta
from collections import deque

#########################
# Ad logs generator 
#
# Help : python botgen.py -h
#
# Examples : 
# Generate log for 2 bots, 10000 users, 10 categories  for 10 seconds since now
#   python botgen.py --bots 2 --users 10000 -cats 10 --duration 10
# Generate log for 1 bot, 100 users, 10 categories for 10 seconds (default), append to file
#   python botgen.py -f data.json
#
# Notes : 
#  bots have ip 172.20.X.X
#  users have ip 172.10.X.X
#

def random_user(): return random.choice(users)
def random_bot(): return random.choice(bots)
def random_cat(): return random.choice(categories)
def random_action(): return random.choice(actions)

def user2ip(id): return "172.10.{}.{}".format(int(id / 255), id % 255)
def bot2ip(id): return "172.20.{}.{}".format(int(id / 255), id % 255)
def rsec(dt): return int(dt.timestamp())   #+ timedelta(seconds = random.randint(0, 59))

def repack2Json(ls):
    return list(map(lambda x: {'unix_time' : x[0], 'category_id': x[1], 'ip' : x[2], 'type' : x[3]}, ls))

def writeAsJson(ls, fname = None):
    if fname == None:
        print(json.dumps(repack2Json(ls), ensure_ascii=False, separators=(',', ':'), indent=1))
    else: 
        with open(fname, 'a+') as fp:
            for item in repack2Json(ls):
                fp.write(json.dumps(item)+"\n")
 
def get_bots_requests(ts, i): 
    if i % 5 == 0:
        return [(str(rsec(ts)), random_cat(), bot2ip(random_bot()), actions[0]) 
            for x in range(len(bots))]
    else:
        return [(str(rsec(ts)), random_cat(), bot2ip(random_bot()), random_action()) 
            for x in range(len(bots))]

def users_requests(ts, i): 
    if i % 10 == 0:
        return [(str(rsec(ts)), random_cat(), user2ip(users[u]), actions[0]) for u in range(len(users))]

    users.rotate(i % len(users))
    return [(str(rsec(ts)), random_cat(), user2ip(random_user()), random_action()) 
        for x in random.sample(users, int(len(users) / 4))]

def on_time_slot(args, ts, i):    
    writeAsJson(users_requests(ts, i), args.file)
    #writeAsJson(get_bots_requests(ts, i), args.file)

def gen_timeslots(args, start_time):
    t1, t2 = start_time, start_time + timedelta(seconds = args.duration)
    counter = 0
    while t1 < t2: 
        on_time_slot(args, t1, counter)
        t1 += timedelta(seconds = 1)
        counter += 1

def main(args):
    global users, bots, categories, actions
    users = deque(range(0, 0 + args.users)) 
    bots = deque(range(250, 250 + args.bots)) 
    categories = deque(range(1000, 1000 + args.cats)) 
    actions = ["view", "click"]

    print("started with parameters :", args)  

    gen_timeslots(args, datetime.now())

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--bots',     type=int, default=2,    help="number of bots")
    parser.add_argument('-u', '--users',    type=int, default=100, help="number of users")
    parser.add_argument('-c', '--cats',     type=int, default=10,   help="number of categories")
    parser.add_argument('-d', '--duration', type=int, default=10,   help="duration in sec")
    parser.add_argument('-f', '--file',     type=str, default=None,   help="write to file")
    args = parser.parse_args()

    main(args)
 