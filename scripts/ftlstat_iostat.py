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
y = np.linspace(0,2000,60)
y1 = np.linspace(0,2000,60)
y2 = np.linspace(0,2000,60)
y3 = np.linspace(0,2000,60)

# dynamic plot xx that is based on x, each sampling append one
xx = []
y_ftl_r = []
y_ftl_w = []

y_nvc_r = []
y_nvc_w = []

y_qlc_r = []
y_qlc_w = []
# dynamic plot y_ratio that is based on x, each sampling append one



def div_zero(numer, denom, default):
    return default if int(denom) is 0 else (numer / denom)

def check_positive(value):
    v = int(value)
    if v <= 0:
        raise argparse.ArgumentTypeError("%s should be positive int value" % v)
    return v


def get_bdev_stat(client, name):
    return rpc.bdev.bdev_get_iostat(client, name=name)


#{'tick_rate': 2800000000, 'ticks': 12503635401812801, 'bdevs': [{'name': 'ftl_0_qlc0n1', 'bytes_read': 268509184, 'num_read_ops': 4100, 'bytes_written': 813062619136, 'num_write_ops': 12406351, 'bytes_unmapped': 0, 'num_unmap_ops': 0, 'read_latency_ticks': 75006694568, 'write_latency_ticks': 9631852791058, 'unmap_latency_ticks': 0}]}
# for k, value in dictionary.items():
#            if k == 'name':
#                self.bdev_name = value
#            elif k == 'bytes_read':
#                self.rd_sectors = value >> 9
#            elif k == 'bytes_written':
#                self.wr_sectors = value >> 9
# (_stat.rd_sectors - _last_stat.rd_sectors) / upt / unit), unit = 2048 for MB display

unit = 2048 # for MB display
#qlc
qlc_upt_last = 0
qlc_rd_sectors_last = 0
qlc_wr_sectors_last = 0 

#nvc
nvc_upt_last = 0
nvc_rd_sectors_last = 0
nvc_wr_sectors_last = 0 

def animate(i):  
    print(i)
    global qlc_upt_last 
    global qlc_rd_sectors_last 
    global qlc_wr_sectors_last  

    global nvc_upt_last 
    global nvc_rd_sectors_last 
    global nvc_wr_sectors_last  

    #
    #qlc
    #
    _stat = get_bdev_stat(args.client, "ftl_0_qlc0n1")
        #print(_stat)
    upt_cur = _stat["ticks"]
    upt_rate = _stat["tick_rate"]
    #print(upt_cur)

    upt = (upt_cur - qlc_upt_last) / upt_rate
    #print(upt)

    qlc_upt_last = _stat["ticks"]

        #print(_stat["bdevs"])

    iostat = _stat["bdevs"]

    print(iostat[0])

        #print(iostat.get('bytes_read'))
        #print_dict(iostat)


    qlc_rd_sectors_cur = _stat["bdevs"][0].get('bytes_read') >> 9
    qlc_wr_sectors_cur = _stat["bdevs"][0].get('bytes_written') >> 9

    #print(rd_sectors_cur)

    qlc_r_mb_s = (qlc_rd_sectors_cur - qlc_rd_sectors_last)/upt/unit
    qlc_w_mb_s = (qlc_wr_sectors_cur - qlc_wr_sectors_last)/upt/unit

    y_qlc_r.append(qlc_r_mb_s)
    y_qlc_w.append(qlc_w_mb_s)
       #print_dict(y_qlc_r);
       #print_dict(y_qlc_w);

    qlc_rd_sectors_last = qlc_rd_sectors_cur
    qlc_wr_sectors_last = qlc_wr_sectors_cur


    #
    #nvc
    #
    _stat = get_bdev_stat(args.client, "nvc")
        #print(_stat)
    upt_cur = _stat["ticks"]
    upt_rate = _stat["tick_rate"]
    #print(upt_cur)

    upt = (upt_cur - nvc_upt_last) / upt_rate
    #print(upt)

    nvc_upt_last = _stat["ticks"]

    print(_stat["bdevs"])

    iostat = _stat["bdevs"]

    print(iostat[0])

        #print(iostat.get('bytes_read'))
        #print_dict(iostat)


    nvc_rd_sectors_cur = _stat["bdevs"][0].get('bytes_read') >> 9
    nvc_wr_sectors_cur = _stat["bdevs"][0].get('bytes_written') >> 9

    #print(rd_sectors_cur)

    nvc_r_mb_s = (nvc_rd_sectors_cur - nvc_rd_sectors_last)/upt/unit
    nvc_w_mb_s = (nvc_wr_sectors_cur - nvc_wr_sectors_last)/upt/unit

    y_nvc_r.append(nvc_r_mb_s)
    y_nvc_w.append(nvc_w_mb_s)
       #print_dict(y_qlc_r);
       #print_dict(y_qlc_w);

    nvc_rd_sectors_last = nvc_rd_sectors_cur
    nvc_wr_sectors_last = nvc_wr_sectors_cur

    #
    #below is plot
    #

    xx.append(x[i])
    line_iostat_qlc_r.set_xdata(xx)
    line_iostat_qlc_r.set_ydata(y_qlc_r)

    line_iostat_qlc_w.set_xdata(xx)
    line_iostat_qlc_w.set_ydata(y_qlc_w)

    line_iostat_nvc_r.set_xdata(xx)
    line_iostat_nvc_r.set_ydata(y_nvc_r)

    line_iostat_nvc_w.set_xdata(xx)
    line_iostat_nvc_w.set_ydata(y_nvc_w)

    return line_iostat_qlc_r, line_iostat_qlc_w,  line_iostat_nvc_r, line_iostat_nvc_w#line_busy, line_idle #,line12


def init():
    line_iostat_qlc_r.set_xdata([])
    line_iostat_qlc_r.set_ydata([])

    line_iostat_qlc_w.set_xdata([])
    line_iostat_qlc_w.set_ydata([])

    line_iostat_nvc_r.set_xdata([])
    line_iostat_nvc_r.set_ydata([])

    line_iostat_nvc_w.set_xdata([])
    line_iostat_nvc_w.set_ydata([])

    return line_iostat_qlc_r, line_iostat_qlc_w,  line_iostat_nvc_r, line_iostat_nvc_w#line_busy,line_idle

#create fig
fig = plt.figure()
#add sub plot create ax to plot
ax1 = fig.add_subplot(2,1,1)
#create line for ratio by ax1.plot
line_iostat_qlc_r, = ax1.plot(x, y,linestyle='dashed', animated=False, label='qlc_r')
line_iostat_qlc_w, = ax1.plot(x, y1,linestyle='dashed', animated=False, label='qlc_w')
line_iostat_nvc_r, = ax1.plot(x, y2,linestyle='dashed', animated=False, label='nvc_r')
line_iostat_nvc_w, = ax1.plot(x, y3,linestyle='dashed', animated=False, label='nvc_w')

legend = plt.legend([line_iostat_qlc_r, line_iostat_qlc_w,line_iostat_nvc_r,line_iostat_nvc_w], 
                    ["qlc_r", "qlc_w", "nvc_r", "nvc_w"], facecolor='blue')

ani = animation.FuncAnimation(fig=fig,
                              func=animate,
                              frames=60,
                              init_func=init,
                              interval=5*1000, #60 second modify here to change sampling rate
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

