from gevent import monkey;monkey.patch_all(thread=False)

import os
import time
import random
import traceback
from typing import Callable
from gevent import Greenlet
from implements.dumbo_node import DumboBFTNode
from implements.bdt_node import BdtBFTNode
from implements.rbcbdt_node import RbcBdtBFTNode
from implements.rotatinghotstuff_node import RotatingHotstuffBFTNode
from implements.ng_k_s_node import NGSNode
from implements.sdumbo_node import SDumboBFTNode
from network import socket_client_ng, socket_client
from network.socket_server import NetworkServer
from multiprocessing import Value as mpValue, Queue as mpQueue
from ctypes import c_bool


def instantiate_bft_node(sid, i, B, N, f, K, S, T, bft_from_server: Callable, bft_to_client: Callable, ready: mpValue,
                         stop: mpValue, protocol="mule", mute=False, F=100, debug=False, omitfast=False, countpoint=0):
    bft = None
    if protocol == 'dumbo':
        bft = DumboBFTNode(sid, i, B, N, f, bft_from_server, bft_to_client, ready, stop, K, mute=mute, debug=debug)
    elif protocol == "bdt":
        bft = BdtBFTNode(sid, i, S, T, B, F, N, f, bft_from_server, bft_to_client, ready, stop, K, mute=mute, omitfast=omitfast)
    elif protocol == "rbc-bdt":
        bft = RbcBdtBFTNode(sid, i, S, T, B, F, N, f, bft_from_server, bft_to_client, ready, stop, K, mute=mute, omitfast=omitfast)
    elif protocol == 'sdumbo':
        bft = SDumboBFTNode(sid, i, B, N, f, bft_from_server, bft_to_client, ready, stop, K, mute=mute, debug=debug)
    elif protocol == 'ng':
        bft = NGSNode(sid, i, S, B, F, N, f, bft_from_server, bft_to_client, ready, stop, mute=mute, countpoint=countpoint)
    elif protocol == "hotstuff":
        bft = RotatingHotstuffBFTNode(sid, i, S, T, B, F, N, f, bft_from_server, bft_to_client, ready, stop, K,
                                      mute=mute, omitfast=omitfast)
    else:
        print("no such a BFT protocol.")
    return bft


if __name__ == '__main__':

    from utils import arg_parser

    args = arg_parser.parse()

    # Some parameters
    sid = args.sid
    i = args.id
    N = args.N
    f = args.f
    B = args.B
    K = args.K
    S = args.S
    T = args.T
    P = args.P
    M = args.M
    F = args.F
    D = args.D
    O = args.O
    C = args.C

    # Random generator
    rnd = random.Random(sid)

    # Nodes list
    addresses = [None] * N
    try:
        with open(os.path.join(os.getcwd(), 'config/hosts.config'), 'r') as hosts:
            for line in hosts:
                params = line.split()
                pid = int(params[0])
                priv_ip = params[1]
                pub_ip = params[2]
                port = int(params[3])
                # print(pid, ip, port)
                if pid not in range(N):
                    continue
                if pid == i:
                    my_address = (priv_ip, port)
                addresses[pid] = (pub_ip, port)
        assert all([node is not None for node in addresses])
        print("hosts.config is correctly read")

        client_bft_mpq = mpQueue()
        client_from_bft = lambda: client_bft_mpq.get(timeout=0.00001)
        bft_to_client = client_bft_mpq.put_nowait

        server_bft_mpq = mpQueue()
        bft_from_server = lambda: server_bft_mpq.get(timeout=0.00001)
        server_to_bft = server_bft_mpq.put_nowait

        client_ready = mpValue(c_bool, False)
        server_ready = mpValue(c_bool, False)
        net_ready = mpValue(c_bool, False)
        stop = mpValue(c_bool, False)

        if P == 'ng':
            net_client = socket_client_ng.NetworkClient(my_address[1], my_address[0], i, addresses,
                                                                client_from_bft, client_ready, stop)
        else:
            net_client = socket_client.NetworkClient(my_address[1], my_address[0], i, addresses, client_from_bft, client_ready, stop)
        net_server = NetworkServer(my_address[1], my_address[0], i, addresses, server_to_bft, server_ready, stop)
        bft = instantiate_bft_node(sid, i, B, N, f, K, S, T, bft_from_server, bft_to_client, net_ready, stop, P, M, F, D, O, C)

        net_server.start()
        net_client.start()

        while not client_ready.value or not server_ready.value:
            time.sleep(1)
            print("waiting for network ready...")

        with net_ready.get_lock():
            net_ready.value = True

        bft_thread = Greenlet(bft.run)
        bft_thread.start()
        bft_thread.join()
        print("BFT finished. Terminating net servers...")

        with stop.get_lock():
            stop.value = True

        net_client.terminate()
        net_client.join()
        time.sleep(1)
        net_server.terminate()
        net_server.join()
        print("Servers terminated.")

    except FileNotFoundError or AssertionError as e:
        traceback.print_exc()
