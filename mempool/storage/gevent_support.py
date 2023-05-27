import gevent


def gevent_support(func):
    def tx_store(*args, **kwargs):
        func(*args, **kwargs)
        gevent.sleep(0)
    return tx_store
