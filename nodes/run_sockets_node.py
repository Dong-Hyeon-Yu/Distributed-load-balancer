from gevent import monkey;monkey.patch_all(thread=False)

from implements.dl_bmr_sockets_node import DL2Node
from network.sockets_client import NetworkClients
from network.sockets_server import NetworkServers
from mempool.mempool_client import MempoolClient
from mempool.mempool import Mempool
from mempool.storage.queue_tx_storage import QueueTxStorage
import time
import random
import traceback
from typing import Callable
from gevent import Greenlet

from multiprocessing import Value as mpValue, Queue as mpQueue, Pipe
from ctypes import c_bool


def instantiate_bft_node(sid, i, B, N, f, K, S, bft_from_server1: Callable, bft_to_client1: Callable,
                         bft_from_server2: Callable, bft_to_client2: Callable, ready: mpValue, stop: mpValue, tx_storage: MempoolClient,
                         protocol="mule", mute=False, F=100, debug=False, omitfast=False, unbalanced_workload=False):
    bft = None
    if protocol == 'dl':
        bft = DL2Node(sid, i, S, B, F, N, f, bft_from_server1, bft_to_client1, bft_from_server2, bft_to_client2, ready,
                      stop, tx_storage, K, mute=mute, unbalanced_workload=unbalanced_workload)

    else:
        print("Only support dl")
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
    P = args.P
    M = args.M
    F = args.F
    D = args.D
    O = args.O
    BYTE = args.Y
    unbalanced_workload = args.unbalanced_workload

    # Random generator
    rnd = random.Random(sid)

    # Nodes list
    addresses1 = [None] * N
    addresses2 = [None] * N
    try:
        with open('config/hosts1_config', 'r') as hosts:
            for line in hosts:
                params = line.split()
                pid = int(params[0])
                priv_ip = params[1]
                pub_ip = params[2]
                port1 = int(params[3])
                port2 = int(params[4])
                # print(pid, priv_ip, port1, port2)
                if pid not in range(N):
                    continue
                if pid == i:
                    my_address1 = (priv_ip, port1)
                    my_address2 = (priv_ip, port2)
                addresses1[pid] = (pub_ip, port1)
                addresses2[pid] = (pub_ip, port2)
        assert all([node is not None for node in addresses1])
        assert all([node is not None for node in addresses2])
        print("hosts.config is correctly read")
    except FileNotFoundError or AssertionError as e:
        traceback.print_exc()

    client_bft_mpq1 = mpQueue()
    client_from_bft1 = lambda: client_bft_mpq1.get(timeout=0.00001)

    client_bft_mpq2 = mpQueue()
    client_from_bft2 = lambda: client_bft_mpq2.get(timeout=0.00001)

    bft_to_client1 = client_bft_mpq1.put_nowait
    bft_to_client2 = client_bft_mpq2.put_nowait

    server_bft_mpq1 = mpQueue()
    #bft_from_server = server_bft_mpq.get
    bft_from_server1 = lambda: server_bft_mpq1.get(timeout=0.00001)
    server_to_bft1 = server_bft_mpq1.put_nowait

    server_bft_mpq2 = mpQueue()
    #bft_from_server = server_bft_mpq.get
    bft_from_server2 = lambda: server_bft_mpq2.get(timeout=0.00001)
    server_to_bft2 = server_bft_mpq2.put_nowait

    client_ready1 = mpValue(c_bool, False)
    server_ready1 = mpValue(c_bool, False)
    client_ready2 = mpValue(c_bool, False)
    server_ready2 = mpValue(c_bool, False)
    net_ready = mpValue(c_bool, False)
    stop = mpValue(c_bool, False)

    net_server1 = NetworkServers(my_address1[1], my_address2[1], my_address1[0], my_address2[0], i, addresses1, addresses2,
                                server_to_bft1, server_to_bft2, server_ready1, server_ready2, stop, stop, 1, 2)
    net_client1 = NetworkClients(my_address1[1], my_address2[1], my_address1[0], my_address2[0], i, addresses1, addresses2,
                                 client_from_bft1, client_from_bft2, client_ready1, client_ready2, stop, stop, BYTE, 0, 1)

    bft_from_mempool, mempool_to_bft = Pipe(duplex=False)
    mempool_from_bft, bft_to_mempool = Pipe(duplex=False)
    mempool_ready = mpValue(c_bool, False)
    tx_storage = MempoolClient(bft_from_mempool, bft_to_mempool)
    mempool_ = Mempool(i, QueueTxStorage(), mempool_from_bft, mempool_to_bft, mempool_ready, stop)

    bft = instantiate_bft_node(sid, i, B, N, f, K, S, bft_from_server1, bft_to_client1,
                               bft_from_server2, bft_to_client2, net_ready, stop, tx_storage, P, M, F, D, O, unbalanced_workload)

    net_server1.start()
    net_client1.start()

    while not client_ready1.value and not server_ready1.value \
            and not client_ready2.value and not server_ready2.value:
        time.sleep(1)
        print("waiting for network ready...")

    with net_ready.get_lock():
        net_ready.value = True

    print("waiting for mempool ready...")
    mempool_.start()
    while not mempool_ready.value:
        time.sleep(1)
    print("mempool ok...")

    bft_thread = Greenlet(bft.run)
    bft_thread.start()
    bft_thread.join()

    with stop.get_lock():
        stop.value = True

    mempool_.terminate()
    mempool_.join()

    net_client1.terminate()
    net_client1.join()

    time.sleep(1)

    net_server1.terminate()
    net_server1.join()
