
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
from mempool.mempool import Mempool
from mempool.mempool_client import MempoolClient
from mempool.storage.dict_tx_storage import DictTxStorage
from mempool.storage.queue_tx_storage import QueueTxStorage
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

    client_bft_mpq = mpQueue()
    client_from_bft = lambda: client_bft_mpq.get(timeout=0.00001)
    bft_to_client = client_bft_mpq.put_nowait

    server_bft_mpq = mpQueue()
    bft_from_server = lambda: server_bft_mpq.get(timeout=0.00001)
    server_to_bft = server_bft_mpq.put_nowait

    client_ready = mpValue(c_bool, False)
    server_ready = mpValue(c_bool, False)
    net_ready = mpValue(c_bool, False)
    bft_stop = mpValue(c_bool, False)

    bft_from_mempool, mempool_to_bft = Pipe(duplex=False)
    mempool_from_bft, bft_to_mempool = Pipe(duplex=False)
    mempool_ready = mpValue(c_bool, False)
    tx_storage = MempoolClient(bft_from_mempool, bft_to_mempool)
    mempool_ = Mempool(i, QueueTxStorage(), mempool_from_bft, mempool_to_bft, mempool_ready, bft_stop)
    if P == 'ng':
        net_client = socket_client_ng.NetworkClient(my_address[1], my_address[0], i, addresses,
                                                            client_from_bft, client_ready, bft_stop)
    else:
        net_client = socket_client.NetworkClient(my_address[1], my_address[0], i, addresses, client_from_bft, client_ready, bft_stop)
    net_server = NetworkServer(my_address[1], my_address[0], i, addresses, server_to_bft, server_ready, bft_stop)
    bft = instantiate_bft_node(sid, i, B, N, f, K, S, T, bft_from_server, bft_to_client, net_ready, bft_stop, tx_storage, P, M, F, D, O, unbalanced_workload)

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

    bft_thread = Greenlet(bft.run)
    bft_thread.start()
    bft_thread.join()
    print("BFT finished. Terminating net servers...")

    with bft_stop.get_lock():
        bft_stop.value = True

    mempool_.terminate()
    mempool_.join()
    net_client.terminate()
    net_client.join()
    time.sleep(1)
    net_server.terminate()
    net_server.join()
    print("Servers terminated.")


