import numpy as np
from matplotlib import pyplot as plt
# from scipy import special


def _zipf(a: np.float64, the_number_of_nodes: np.uint64):
    """
        Generate Zipf-like random variables,
        but in inclusive [1...the_number_of_nodes] interval
    """
    s = np.random.zipf(a, (100_000, the_number_of_nodes))
    count, bins, ignored = plt.hist(s[s < the_number_of_nodes+1], the_number_of_nodes, density=True)

    # x = np.arange(1., the_number_of_nodes+1)
    # y = x ** (-a) / special.zetac(a)  # from scipy import special
    # plt.plot(x, y / max(y), linewidth=2, color='r')
    # plt.title(f"Zipf (a={a}, n={the_number_of_nodes})")
    # plt.show()

    return count


def zipfian_coefficient(node_id: int, the_number_of_nodes: np.uint64, a: np.float64 = 1.015625) -> int:
    """
        get the number of loads for a node
    """
    return _zipf(a, the_number_of_nodes)[node_id]


if __name__ == "__main__":
    """
        try to draw a histogram showing zipfian distribution with the skewness 'a'
    """
    N = 4
    a = 1.015625
    _zipf(1.015625, N)
    # for i in range(N):
    #     print(zipfian_workload(i, a, N, 1_000_000))

