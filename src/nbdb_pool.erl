-module(nbdb_pool).
-behaviour(gen_server).

-export([start_link/0, start_link/2]).

-export([init/1, handle_call/3, handle_cast/2]).

-export([start/2,add/2, get_connection/2]).

-define(DEFAULTPOOL, default_pool).

-record(state,{pool, connector, max_idle, min_idle, max_total, min_total, total_connections=0, in_use_map=#{}, idle_list=[], wait_queue=queue:new()}).

%% API =========================================================================================

start(Pool, PoolConfig) ->
    io:format("(nbdb_pool:start/2) ....~n"),
    nbdb_pool_sup:start_child(Pool, PoolConfig).

add(PoolRef,Connection) ->
    gen_server:cast(PoolRef,{add_connection, Connection}).


get_connection(Pool, Timeout) ->
    gen_server:call(Pool, {get_connection, Timeout}, Timeout).

%% OTP =========================================================================================



start_link() ->
    io:format("(nbdb_pool:start_link/0)~n"),
    start_link(?DEFAULTPOOL,#{}).

start_link(Pool,PoolConfig) ->
    io:format("(nbdb_pool:start_link/2) for pool ~p~n",[Pool]),
    gen_server:start_link({local, Pool}, nbdb_pool, {Pool,PoolConfig}, []).

init({Pool,PoolConfig}) ->
    io:format("(nbdb_pool:init) starting nbdb_pool for pool ~p~n",[Pool]),
    %% read open and close function from config
    PoolConnector = list_to_atom(atom_to_list(Pool) ++ "_connector"),
    State = #state{pool=Pool,connector=PoolConnector},
    {ok, State}.


%%----------------------
handle_cast({add_connection, Connection}, State0=#state{total_connections=TC}) ->
    io:format("(~p:~p), handle_cast add_connection for pool ~p~n",[?MODULE,State0#state.pool,State0#state.pool]),
   
    erlang:link(Connection), 
    State=process_unused_connection(Connection,State0#state{total_connections=(TC+1)}),

    {noreply, State};

handle_cast(UnknownCast, State) ->
    io:format("(~p:~p), handle_cast, unknown cast: ~p~n",[?MODULE,State#state.pool,UnknownCast]),
    {noreply, State}.


%%----------------------

handle_call({get_connection, Timeout}, Client={Pid,_}, State0=#state{total_connections=TC, max_total=Max, wait_queue=WQ}) ->
    io:format("(~p:~p), handle_call:get_connection~n",[?MODULE,State0#state.pool]),
    State2 = case get_from_idle_list(State0) of
      {ok, Connection, State1}  -> deliver_to_client(Connection, Client, State1);
      _			       -> case TC < Max of true -> get_extra_connections(1,State0) end,
                                  State0#state{wait_queue=queue:in({Client,(now_milli_secs()+Timeout)},WQ)}
    end,
    {noreply, State2};



handle_call(Request, From, State) ->
    io:format("~p: got call for unknown request ~p from ~p~n",[State#state.pool, Request, From]),
    {reply, error, State}.

%% INTERNAL ======================================================================================



deliver_to_client(Connection, Client={Pid,_}, State=#state{in_use_map=IUM}) ->
    gen_server:reply(Client, {ok, Connection}),
    NewEntry = case maps:find(Pid,IUM) of		%% a single client can have multiple connections in use
      {ok, Value} -> [Connection|Value];
      _		  -> erlang:link(Pid),  %% to get notified when client dies so we can reuse the Connection
                     [Connection]
    end,
    State#state{in_use_map=IUM#{Pid=>NewEntry}}.

return_from_client(Connection, Pid, State=#state{in_use_map=IUM}) ->
    NewEntry = case maps:find(Pid,IUM) of
      {ok, List} -> lists:delete(Connection,List);
       _ -> []
    end,
    case NewEntry of
        [] -> erlang:unlink(Pid), 
              State#state{in_use_map=maps:remove(Pid,IUM)};
        _  -> State#state{in_use_map=IUM#{Pid=>NewEntry}}
    end.


add_to_idle_list(Connection, State=#state{idle_list=Idle}) ->
    State#state{idle_list=Idle++[{Connection, now_milli_secs()}]}.

get_from_idle_list(State=#state{idle_list=[]}) ->
    {empty, State};
get_from_idle_list(State=#state{idle_list=[{Connection,_}|Idle]}) ->
    {ok, Connection, State#state{idle_list=Idle}}.

    
process_unused_connection(Connection, State=#state{idle_list=Idle,wait_queue=WQ}) ->
    case queue:out(WQ) of
      {empty, _} -> add_to_idle_list(Connection, State);
      {{value, {Client,Timeout}}, NewQ} -> case Timeout < now_milli_secs() of
			true -> process_unused_connection(Connection, State#state{wait_queue=NewQ});
                        _    -> deliver_to_client(Connection,Client,  State#state{wait_queue=NewQ})
                end
    end.



recalculate_total_connections(State=#state{in_use_map=IUM,idle_list=Idle}) ->
    State#state{total_connections=(length(Idle) + maps:fold(fun(_Key,Value,AccIn) -> AccIn+length(Value) end, 0, IUM))}.

get_extra_connections(Number, State=#state{connector=PoolConnector}) ->
    nbdb_connector:request(PoolConnector,Number).

now_milli_secs() ->
  erlang:monotonic_time(milli_seconds).




