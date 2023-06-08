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
from mempool.load_balancing.load_balancer import LoadBalancer
from mempool.storage.dict_tx_storage import DictTxStorage
from mempool.storage.queue_tx_storage import QueueTxStorage
from mempool.mempool import Mempool
from mempool.mempool_client import MempoolClient
from network import socket_client_ng, socket_client
from network.socket_server import NetworkServer
from multiprocessing import Value as mpValue, Queue as mpQueue, Pipe
from ctypes import c_bool


def instantiate_bft_node(sid, i, B, N, f, K, S, T, bft_from_server: Callable, bft_to_client: Callable, ready: mpValue,
                         stop: mpValue, tx_storage: MempoolClient, protocol="mule", mute=False, F=100, debug=False,
                         omitfast=False, unbalanced_workload=False):
    bft = None
    if protocol == 'dumbo':
        bft = DumboBFTNode(sid, i, B, N, f, bft_from_server, bft_to_client, ready, stop, tx_storage, K, mute=mute,
                           debug=debug, unbalanced_workload=unbalanced_workload)

    elif protocol == "bdt":
        bft = BdtBFTNode(sid, i, S, T, B, F, N, f, bft_from_server, bft_to_client, ready, stop, tx_storage, mode=K,
                         mute=mute, omitfast=omitfast, unbalanced_workload=unbalanced_workload)

    elif protocol == "rbc-bdt":
        bft = RbcBdtBFTNode(sid, i, S, T, B, F, N, f, bft_from_server, bft_to_client, ready, stop, tx_storage, K, mute=mute,
                            omitfast=omitfast, unbalanced_workload=unbalanced_workload)

    elif protocol == 'sdumbo':
        bft = SDumboBFTNode(sid, i, B, N, f, bft_from_server, bft_to_client, ready, stop, tx_storage, K, mute=mute,
                            debug=debug, unbalanced_workload=unbalanced_workload)

    elif protocol == 'ng':
        bft = NGSNode(sid, i, S, B, F, N, f, bft_from_server, bft_to_client, ready, stop, mute=mute)

    elif protocol == "hotstuff":
        bft = RotatingHotstuffBFTNode(sid, i, S, T, B, F, N, f, bft_from_server, bft_to_client, ready, stop, tx_storage,
                                      K, mute=mute, omitfast=omitfast, unbalanced_workload=unbalanced_workload)
    else:
        print("no such a BFT protocol.")
    return bft


if __name__ == '__main__':
    from gevent import monkey
    monkey.patch_all(thread=False)
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
    unbalanced_workload = args.unbalanced_workload

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
    except FileNotFoundError or AssertionError as e:
        traceback.print_exc()

    # bft node <- web client
    client_bft_mpq = mpQueue()
    client_from_bft = lambda: client_bft_mpq.get(timeout=0.00001)
    bft_to_client = client_bft_mpq.put_nowait  # Todo: problematic?

    # bft node -> web server
    server_bft_mpq = mpQueue()
    bft_from_server = lambda: server_bft_mpq.get(timeout=0.00001)
    server_to_bft = server_bft_mpq.put_nowait

    # shared variables for inter-process-communication
    client_ready = mpValue(c_bool, False)
    server_ready = mpValue(c_bool, False)
    net_ready = mpValue(c_bool, False)
    stop = mpValue(c_bool, False)

    # load-balancing values
    lb_ready = mpValue(c_bool, False)
    lb_stop = mpValue(c_bool, False)
    server_lb_mpq = mpQueue()
    lb_from_server = lambda: server_lb_mpq.get(timeout=0.00001)
    server_to_lb = server_lb_mpq.put_nowait

    bft_from_mempool, mempool_to_bft = Pipe(duplex=False)
    mempool_from_bft, bft_to_mempool = Pipe(duplex=False)
    lb_from_mempool, mempool_to_lb = Pipe(duplex=False)
    mempool_from_lb, lb_to_mempool = Pipe(duplex=False)

    mempool_ready = mpValue(c_bool, False)
    mempool_ = Mempool(i, QueueTxStorage(), mempool_from_bft, mempool_to_bft, mempool_ready, stop, mempool_from_lb, mempool_to_lb)

    load_balancer = LoadBalancer(  # lb_to_client -> bft_to_client
        i, N, 3, B, MempoolClient(lb_from_mempool, lb_to_mempool), stop, lb_ready, client_bft_mpq, lb_from_server)

    if P == 'ng':
        net_client = socket_client_ng.NetworkClient(my_address[1], my_address[0], i, addresses,
                                                            client_from_bft, client_ready, stop)
    else:
        net_client = socket_client.NetworkClient(my_address[1], my_address[0], i, addresses, client_from_bft,
                                                 client_ready, stop)
    net_server = NetworkServer(my_address[1], my_address[0], i, addresses, server_to_bft, server_ready, stop, server_to_lb)

    bft = instantiate_bft_node(sid, i, B, N, f, K, S, T, bft_from_server, bft_to_client, net_ready, stop,
                               MempoolClient(bft_from_mempool, bft_to_mempool), P, M, F, D, O, unbalanced_workload)

    # activate web server & client
    net_server.start()
    net_client.start()
    while not client_ready.value or not server_ready.value:
        time.sleep(1)
        print("waiting for network ready...")

    with net_ready.get_lock():
        net_ready.value = True
    print("network ok...")

    print("waiting for mempool ready...")
    mempool_.start()
    while not mempool_ready.value:
        time.sleep(1)
    print("mempool ok...")

    # activate load-balancer
    load_balancer.start()
    while not lb_ready:
        time.sleep(1)
        print("waiting for load balancer ready...")

    bft_thread = Greenlet(bft.run)
    bft_thread.start()
    bft_thread.join()
    print("BFT finished. Terminating net servers...")

    with stop.get_lock():
        stop.value = True

    load_balancer.terminate()
    load_balancer.join()
    print("Load balancer terminated.")
    with lb_stop.get_lock():
        lb_stop.value = True

    mempool_.terminate()
    mempool_.join()

    time.sleep(1)
    net_client.terminate()
    net_client.join()
    time.sleep(1)
    net_server.terminate()
    net_server.join()
    print("Servers terminated.")
