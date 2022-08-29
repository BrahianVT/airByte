#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_eth_infura import SourceEthInfura

if __name__ == "__main__":
    source = SourceEthInfura()
    launch(source, sys.argv[1:])
