-module(episcina).



-export([get_connection/2, return_connection/2]).






get_connection(Pool, Timeout) ->
    nbdb:get_connection(Pool, Timeout).

return_connection(Pool, DB) ->
    nbdn:return_conneciton(Pool, DB).


