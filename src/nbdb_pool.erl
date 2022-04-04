-module(nbdb_pool).
-behaviour(gen_server).

-export([start_link/0, start_link/1]).

-export([init/1, handle_call/3, handle_cast/2]).

-export([start/1,add/2]).

-define(DEFAULTPOOL, default_pool).

-record(state,{pool, connector, max_idle, min_idle, max_total, min_total, in_use_map, idle_list, wait_queue}).

%% API =========================================================================================

start(Pool) ->
    nbdb_pool_sup:start_child(Pool).

add(PoolRef,Connection) ->
    gen_server:cast(PoolRef,{add_connection, Connection}).


%% OTP =========================================================================================



start_link() ->
    start_link(?DEFAULTPOOL).

start_link(Pool) ->
    io:format("~p, start_link for ~p~n",[?MODULE, Pool]),
    gen_server:start_link({local, Pool}, nbdb_pool, [Pool], []).

init([Pool]) ->
    io:format("starting nbdb_pool for pool ~p~n",[Pool]),
    %% read open and close function from config
    State = #state{pool=Pool},
    {ok, State}.


%%----------------------
handle_cast({add_connection, _Connection}, State) ->
    io:format("handle_cast add_connection for pool ~p~n",[State#state.pool]),
    {noreply, State};

handle_cast(UnknownCast, State) ->
    io:format("handle_cast, unknown cast: ~p~n",[UnknownCast]),
    {noreply, State}.


%%----------------------
handle_call(Request, From, State) ->
    io:format("~p: got call for unknown request ~p from ~p~n",[State#state.pool, Request, From]),
    {reply, error, State}.

%% INTERNAL ======================================================================================
