import os
import pickle
from coincurve import PrivateKey, PublicKey
from crypto.cryptoprimitives.threshsig.boldyreva import TBLSPublicKey, TBLSPrivateKey

KEY_DIR = os.path.join(os.getcwd(), 'crypto', 'keys')


def load_key_(id):

    with open(os.path.join(KEY_DIR, 'sPK.key'), 'rb') as fp:
        sPK = pickle.load(fp)

    with open(os.path.join(KEY_DIR, 'ePK.key'), 'rb') as fp:
        ePK = pickle.load(fp)

    with open(os.path.join(KEY_DIR, f'sSK-{id}.key'), 'rb') as fp:
        sSK = pickle.load(fp)

    with open(os.path.join(KEY_DIR, f'eSK-{id}.key'), 'rb') as fp:
        eSK = pickle.load(fp)

    return sPK, ePK, sSK, eSK


def load_key(id, N):

    sPK, ePK, sSK, eSK = load_key_(id)

    with open(os.path.join(KEY_DIR, 'sPK1.key'), 'rb') as fp:
        sPK1 = pickle.load(fp)

    sPK2s = []
    for i in range(N):
        with open(os.path.join(KEY_DIR, f'sPK2-{i}.key'), 'rb') as fp:
            sPK2s.append(PublicKey(pickle.load(fp)))

    with open(os.path.join(KEY_DIR, f'sSK1-{id}.key'), 'rb') as fp:
        sSK1 = pickle.load(fp)

    with open(os.path.join(KEY_DIR, f'sSK2-{id}.key'), 'rb') as fp:
        sSK2 = PrivateKey(pickle.load(fp))

    return sPK, sPK1, sPK2s, ePK, sSK, sSK1, sSK2, eSK


