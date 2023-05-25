import sys
import os

sys.path.append(os.getcwd())
for _dir in os.listdir(os.getcwd()):
    sys.path.append(os.path.join(os.getcwd(), _dir))

