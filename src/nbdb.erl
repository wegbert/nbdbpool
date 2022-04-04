-module(nbdb).


%% public
-export([test/0, show_config/0, start_pool/1, stop_pool/1, info/0, get_connection/1, get_connection/2, return_connection/2]).

%% private
-export([start_pools/1]).

%% interface for this application




test() ->
    io:format("test~n",[]).


show_config() ->
    What = application:get_env(nbdbpool,pools),
    io:format("what: ~p~n",[What]).
    %%show_config(ConfigPools).
show_config([]) ->
     ok;
show_config([{Pool,Config}|Rest]) ->
     io:format("pool: ~p~n config: ~p~n~n",[Pool,Config]),
     show_config(Rest).


start_pool(_Pool) ->
    ok.


stop_pool(_Pool) ->
    ok.


info() ->
    ok.


get_connection(Pool) ->
    get_connection(Pool,5000).

get_connection(_Pool, _Timeout) ->
    ok.


return_connection(_Pool, _Connection) ->
    ok.



%% private

start_pools([]) ->
    ok;

start_pools(Config) -> 
    Default = proplists:get_value(default,Config),
    io:format(" defaults found: ~p~n",[Default]),
    start_pools(proplists:delete(default,Config), default).

start_pools([],_) ->
    ok;

start_pools([{Pool,ConfigMap}|Rest], Default) ->
    io:format("start pool ~p~n",[Pool]),

    %% create connect/close funs
    Connect_fun = fun(Pool) -> io:format("start new connection for pool ~p~n",[Pool]) end,
    Close_fun = fun(Pool) -> io:format("start close connection for pool ~p~n",[Pool]) end,

    %% start chuld nbdb_connector
    nbdb_connector:start(Pool),

    %% start child nbdb_pool

    PoolConfig = #{
        max_idle => maps:get(max_idle, ConfigMap, Default#{max_idle}),
        min_idle => maps:get(min_idle, ConfigMap, Default#{min_idle}),
    },
    nbdb_pool:start(Pool, PoolConfig),
    
    start_pools(Rest, Default).
