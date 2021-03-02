#!/usr/bin/env python3

import logging
import sys
import argparse
import time
import rpc

import matplotlib.pyplot as plt 
import numpy as np
import matplotlib.animation as animation
from rpc.client import print_dict, print_json, JSONRPCException


#x, y to create plot background well with almost real data
x = np.linspace(0,60,60,dtype=int)
y = np.linspace(0,1,60)

# dynamic plot xx that is based on x, each sampling append one
xx = []
y_busy = []
y_idle = []
# dynamic plot y_ratio that is based on x, each sampling append one
y_ratio = []


busy_last = 0
idle_last = 0

def div_zero(numer, denom, default):
    return default if int(denom) is 0 else (numer / denom)

def check_positive(value):
    v = int(value)
    if v <= 0:
        raise argparse.ArgumentTypeError("%s should be positive int value" % v)
    return v


def get_thread_stat(client):
    return rpc.app.thread_get_stats(args.client)


def animate(i):  
    print(i)
    global busy_last
    global idle_last

    if i == 0:#1st sampling
        _stat = get_thread_stat(args.client)
        dict1 = _stat["threads"]
        #print_dict(dict1);

        for p in dict1:
            if p["name"] == "ftl_core_thread":
                busy_last = p["busy"]
                idle_last = p["idle"]
                y_ratio.append(0)


   

    else: # furthur sampling
        _stat = get_thread_stat(args.client)
        dict1 = _stat["threads"]
        for p in dict1:
            if p["name"] == "ftl_core_thread":
                busy_curr = p["busy"] - busy_last
                idle_curr = p["idle"] - idle_last

                busy_last = p["busy"]
                idle_last = p["idle"]

                ratio_curr = div_zero(busy_curr, idle_curr+busy_curr, 0) #busy_curr/(idle_curr+busy_curr) #
                y_ratio.append(ratio_curr)

    xx.append(x[i])
    line_ratio.set_xdata(xx)
    line_ratio.set_ydata(y_ratio)

    return line_ratio #line_busy, line_idle #,line12


def init():
    line_ratio.set_xdata([])
    line_ratio.set_ydata([])

    return line_ratio #line_busy,line_idle

#create fig
fig = plt.figure()
#add sub plot create ax to plot
ax1 = fig.add_subplot(2,1,1)
#create line for ratio by ax1.plot
line_ratio, = ax1.plot(x, y,linestyle='dashed', animated=False)

  

ani = animation.FuncAnimation(fig=fig,
                              func=animate,
                              frames=60,
                              init_func=init,
                              interval=60*1000, #60 second modify here to change sampling rate
                              blit=False,
                              repeat=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='SPDK ftl cpu stat get command line interface')


    parser.add_argument('-s', "--server", dest='server_addr',
                        help='RPC domain socket path or IP address',
                        default='/var/tmp/spdk.sock')

    parser.add_argument('-p', "--port", dest='port',
                        help='RPC port number (if server_addr is IP address)',
                        default=4420, type=int)

    parser.add_argument('-b', '--name', dest='name',
                        help="Name of the Blockdev. Example: Nvme0n1", required=False)

    parser.add_argument('-o', '--timeout', dest='timeout',
                        help='Timeout as a floating point number expressed in seconds \
                        waiting for response. Default: 60.0',
                        default=60.0, type=float)

    parser.add_argument('-v', dest='verbose', action='store_const', const="INFO",
                        help='Set verbose mode to INFO', default="ERROR")


    args = parser.parse_args()


    parser.print_help()

    args.client = rpc.client.JSONRPCClient(
        args.server_addr, args.port, args.timeout, log_level=getattr(logging, args.verbose.upper()))


  
    plt.show()

