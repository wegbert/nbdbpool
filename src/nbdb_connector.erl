-module(nbdb_connector).
-behaviour(gen_server).

-export([start_link/0, start_link/1]).

-export([init/1, handle_call/3, handle_cast/2]).

-export([start/2, request/1, request/2, request/3]).

-define(DEFAULTPOOL, {default_pool,undef,undef}).

-record(state,{pool,open_fun}).





%% This is a separate gen_server because the "open" function to open a new DB connecition is blocking.
%% If this is done in the main pool gen_server and when very busy, opening a new connection blocks the 
%% retrieval/returning of already active db connections in the pool.
%%
%%
%%





%% API =========================================================================================

start(Pool, ConnectorFun) ->
    nbdb_connector_sup:start_child({Pool, ConnectorFun}).


request(Number) when is_number(Number) ->
    request(?DEFAULTPOOL,Number);

request(PoolConnector) ->
    request(PoolConnector,1).

request(PoolConnector,Number) ->
    io:format("request for ~p extra connections to pool connector ~p~n",[Number,PoolConnector]),
    gen_server:cast(PoolConnector, {request, Number}).

request(PoolConnector,Number,Ref) ->
    io:format("request for ~p extra connections to pool connector ~p~n",[Number,PoolConnector]),
    gen_server:cast(PoolConnector, {request, Number, Ref}).


%% OTP =========================================================================================



start_link() ->
    start_link(?DEFAULTPOOL).

start_link(PoolConnectorArg) ->
    {Pool,_} = PoolConnectorArg,
    io:format("~p, start_link for ~p~n",[?MODULE, Pool]),
    Name = list_to_atom(atom_to_list(Pool) ++ "_connector"),
    gen_server:start_link({local, Name}, nbdb_connector, PoolConnectorArg, []).

init({Pool,Open}) ->
    io:format("starting nbdb_connector for pool ~p~n",[Pool]),
    State = #state{pool=Pool, open_fun=Open},
    {ok, State}.

%%----------------------
handle_cast({request, Number}, State) ->
    io:format("~p: got request to open ~p new database connections~n",[State#state.pool, Number]),
    OpenFun=State#state.open_fun,
    lists:map( fun(_) -> deliver_connection( State#state.pool, OpenFun() ) end, lists:seq(1, Number)),
    {noreply, State};

handle_cast({request, Number, From}, State) ->
    io:format("~p: got request from ~p to open ~p new database connections~n",[State#state.pool, From, Number]),
    OpenFun=State#state.open_fun,
    lists:map( fun(_) -> deliver_connection( From, OpenFun() ) end, lists:seq(1, Number)),
    {noreply, State};

handle_cast(UnknownRequest, State) ->
    io:format("~p: unknown request: ~p~n",[State#state.pool, UnknownRequest]),
    {noreply, State}.

%%----------------------
handle_call(Request, From, State) ->
    io:format("~p: got call for unknown request ~p from ~p~n",[State#state.pool, Request, From]),
    {reply, error, State}.

%% INTERNAL ======================================================================================


%%open_connection(OpenFun) ->
%%    OpenFun().
    

deliver_connection(PoolRef,Connection) ->
    nbdb_pool:add(PoolRef,Connection).
