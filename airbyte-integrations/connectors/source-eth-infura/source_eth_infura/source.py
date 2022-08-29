#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
import json
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator, NoAuth

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


# Basic full refresh stream
class EthInfuraStream(HttpStream, IncrementalMixin, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.
    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalEthInfuraStream((EthInfuraStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """
    url_base = "https://mainnet.infura.io/v3/"
    primary_key = "string"
    cursor_field = "string"
    def __init__(self, config: Mapping[str, Any], start_block: str,**kwargs):
        super().__init__()
        self.api_key = config['api_key']
        self.transaction_details = config['transaction_details']
        self.url_base+=self.api_key
        self.start_block = start_block
        self.body = {
        "jsonrpc":"2.0",
        "method":"eth_getBlockByNumber",
        "params": [start_block, self.transaction_details],
        "id":"1"
        }
        self._cursor_value = None
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        return [response.json()]
        
    def request_body_json(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, 
    ) -> Optional[Mapping]:
        
        if self._cursor_value:
            maxBlock = int(self._cursor_value,16) + 1
            self._cursor_value = hex(maxBlock)
            self.body['params'][0] = self._cursor_value
        return self.body 
        
    
    @property
    def http_method(self) -> str:
        return "POST"
        
    
    @property
    def state(self) -> Mapping[str, Any]:        
        if self._cursor_value:
            return { self.cursor_field: self._cursor_value}
        else:
            self._cursor_value = self.start_block
            return { self.cursor_field: self.start_block }
            
    @state.setter
    def state(self, value: Mapping[str, Any]):
        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@") 
        self._cursor_value = value[self.cursor_field]
        
        
    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            if self._cursor_value:
                latest_record_block = record['result']['number']
                self._cursor_value = latest_record_block
            yield record    
    
       
    def _chunk_date_range(self, start_block : str) -> List[Mapping[str, Any]]:
        """
        Returns a list of each block between the start block and 10 more.
        The return value is a list of dicts {'string': string}.  
        just fr thi connector I want until block 1_500_000
        """
        blocks = []
        start_block = int(start_block,16)
        while start_block < 1_500_000:
            blocks.append({ self.cursor_field: start_block})
            start_block += 1
        return blocks
        
    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        start_block = stream_state[self.cursor_field] if stream_state and self.cursor_field in stream_state else self.start_block
        return self._chunk_date_range(start_block)
        
        
    def backoff_time(self, response: requests.Response) -> Optional[float]:
        if response['error']['message'] == "daily request count exceeded, request rate limited":
            self.max_tries = 0
            return None
        elif "request rate exceeded" in response['error']['message']:
            return float(response['error']['data']['backoff_seconds'])

class Blocks(EthInfuraStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    #primary_key = None

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return ""


# Basic incremental stream
class IncrementalEthInfuraStream(EthInfuraStream):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    


# Source
class SourceEthInfura(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        
        url = 'https://mainnet.infura.io/v3/' + "/" + config["api_key"]
        body = {"jsonrpc":"2.0","method":"eth_chainId","params": [],"id":1}
        try:
            res = requests.post(url,data=json.dumps(body))
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        auth = NoAuth()
        # parse de blok decimal to start to extract from
        start_block = config['hex_number_block']
        return [Blocks(authenticator=auth, config=config, start_block=start_block)]
