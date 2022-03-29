-module(nbdb_connector).
-behaviour(gen_server).

-export([start_link/0, start_link/1]).

-export([init/1, handle_call/3, handle_cast/2]).

-export([request/1, request/2]).

-record(state,{pool,open,close}).

-define(DEFAULTPOOL, default_pool).

request(Number) when is_number(Number) ->
    request(?DEFAULTPOOL,Number);

request(Pool) ->
    request(Pool,1).

request(Pool,Number) ->
    io:format("request for ~p extra connections to pool ~p~n",[Number,Pool]),
    gen_server:cast({nbdb_connector,Pool}, {request, Number, self()}).






start_link() ->
    start_link(?DEFAULTPOOL).

start_link(Pool) ->
    gen_server:start_link({local, {nbdb_connector,Pool}}, nbdb_connector, [Pool], []).


init([Pool]) ->
    io:format("starting nbdb_connector for pool ~p~n",[Pool]),
    %% read open and close function from config
    State = #state{pool=Pool},
    {ok, State}.


handle_cast({request, Number, From}, State) ->
    io:format("~p: got request from ~p to open ~p new database connections~n",[State#state.pool, From, Number]),
    {noreply, State};

handle_cast(UnknownRequest, State) ->
    io:format("~p: unknown request: ~p~n",[State#state.pool, UnknownRequest]),
    {noreply, State}.


handle_call(Request, From, State) ->
    io:format("~p: got call for unknown request ~p from ~p~n",[State#state.pool, Request, From]),
    {reply, error, State}.

