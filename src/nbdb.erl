-module(nbdb).


%% public
-export([test/0, show_config/0, start_pool/1, stop_pool/1, info/0, get_connection/1, get_connection/2, return_connection/2, add_connection/0, add_connection/1, add_connection/2]).

%% private
-export([start_pools/1]).


-define(CONFIGOPTS, #{min_idle=>0,
                      max_idle=>1,
                      max=>1,
                      min=>0,
                      connect_provider=>{io,format,["no connect_provider configured!~n"]},
                      close_provider=>{io,format,["no close_provider configured!~n"]}
}).

%% interface for this application

add_connection() ->
    add_connection(default_pool,1).

add_connection(Number) when is_number(Number) ->
    add_connection(default_pool,Number);

add_connection(Pool) when is_atom(Pool) ->
    add_connection(Pool,1).

add_connection(Pool,Number) ->
    PoolConnector = list_to_atom(atom_to_list(Pool) ++ "_connector"),
    nbdb_connector:request(PoolConnector,Number). 

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
    Default = maps:merge(?CONFIGOPTS,proplists:get_value(default,Config)),
    io:format(" defaults used: ~p~n",[Default]),
    start_pools(proplists:delete(default,Config), Default).

start_pools([],_) ->
    ok;

start_pools([{Pool,ConfigMap}|Rest], Default) ->
    io:format("start pool ~p~n",[Pool]),

    %% #{min_idle:=MinIdle, max_idle:MaxIdle, min:=Min, max:=Max, connect_provider:=ConnectProvider, close_provider:=CloseProvider} = maps:merge(Default,ConfigMap),

    PoolConfig = #{connect_provider:=ConnectProvider, close_provider:=CloseProvider} = maps:merge(Default,ConfigMap),

    %% create connect/close funs
    Connect_fun = create_fun(ConnectProvider),    %% fun(Pool) -> io:format("start new connection for pool ~p~n",[Pool]) end,
    Close_fun = create_fun(CloseProvider),   %%fun(Pool) -> io:format("start close connection for pool ~p~n",[Pool]) end,

    %% start chuld nbdb_connector
    nbdb_connector:start(Pool, Connect_fun),

    %% start child nbdb_pool

    nbdb_pool:start(Pool, PoolConfig#{connect_fun=>Connect_fun,close_fun=>Close_fun}),
    
    start_pools(Rest, Default).

create_fun({Mod,Func,Args}) ->
    fun() -> erlang:apply(Mod,Func,Args) end.

