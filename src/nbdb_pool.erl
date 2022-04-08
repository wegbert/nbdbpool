-module(nbdb_pool).
-behaviour(gen_server).

-export([start_link/0, start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export([start/2,add/2, get_connection/2, return_connection/2]).

-define(DEFAULTPOOL, default_pool).

-record(state,{pool, connector, connect_fun, close_fun, reset_fun, max_idle, min_idle, max_total, min_total, total_connections=0, in_use_map=#{}, idle_list=[], wait_queue=queue:new(), wait_queue_len=0, connection_request_len=0}).

%% API =========================================================================================

start(Pool, PoolConfig) ->
    io:format("(nbdb_pool:start/2) ....~n"),
    nbdb_pool_sup:start_child(Pool, PoolConfig).

add(PoolRef,Connection) ->
    gen_server:cast(PoolRef,{add_connection, Connection, PoolRef}).


get_connection(Pool, Timeout) ->
    try
        gen_server:call(Pool, {get_connection, Timeout+now_milli_secs()}, Timeout)
    catch
        exit:{timeout,_} ->
          {error, timeout};
        _:_ -> {error, unknown}
    end.

return_connection(Pool, Connection) ->
    gen_server:cast(Pool, {return_connection, self(), Connection}).

    

%% OTP =========================================================================================



start_link() ->
    io:format("(nbdb_pool:start_link/0)~n"),
    start_link(?DEFAULTPOOL,#{}).

start_link(Pool,PoolConfig) ->
    io:format("(nbdb_pool:start_link/2) for pool ~p~n",[Pool]),
    gen_server:start_link({local, Pool}, nbdb_pool, {Pool,PoolConfig}, []).

init({Pool,PoolConfig=#{min_idle:=MinIdle,max_idle:=MaxIdle,min:=Min,max:=Max,close_fun:=CloseFun,connect_fun:=ConnectFun,reset_fun:=ResetFun}}) ->
    io:format("(nbdb_pool:init) starting nbdb_pool for pool ~p~n",[Pool]),
    io:format("(nbdb_pool:init) config: ~n==============~n~p~n==============~n",[PoolConfig]),

    erlang:process_flag(trap_exit, true),

    %% read open and close function from config
    PoolConnector = list_to_atom(atom_to_list(Pool) ++ "_connector"),

    %% instruct to do startup things:
    gen_server:cast(self(),init2),
    


    State = #state{pool=Pool,connector=PoolConnector, connect_fun=ConnectFun, close_fun=CloseFun, reset_fun=ResetFun, max_idle=MaxIdle, min_idle=MinIdle, max_total=Max, min_total=Min, connection_request_len=0},
    {ok, State}.


init2(State0) ->
    State1=recalculate_total_connections(State0),
    #state{min_total=Min,total_connections=TC}=State1,
    io:format("Min: ~p, TC: ~p~n",[Min,TC]),
    State = case ((Min-TC) > 0) of			%% what todo with pending connections ()
       true -> get_extra_connections((Min-TC),State1);
       _    -> State1
    end,
    State.

%%----------------------
handle_cast(init2, State) ->
    {noreply, init2(State)};

handle_cast({add_connection, Connection, From}, State0=#state{total_connections=TC, connection_request_len=CRL}) ->
    io:format("(~p:~p), handle_cast add_connection for pool ~p from: ~p~n",[?MODULE,State0#state.pool,State0#state.pool,From]),
   
    erlang:link(Connection),
    Self= self(),
    NewCRL = case From of
        Self -> CRL-1;
        _	-> CRL
    end,
    State=process_unused_connection(Connection,State0#state{total_connections=(TC+1),connection_request_len=NewCRL}),

    {noreply, State};

handle_cast({return_connection, Client, Connection}, State0) ->
    io:format("(~p:~p), handle_cast return_connection for pool ~p~n",[?MODULE,State0#state.pool,State0#state.pool]), 
    State=return_from_client(Connection, Client, State0),
    {noreply, process_unused_connection(Connection,State)};

handle_cast(UnknownCast, State) ->
    io:format("(~p:~p), handle_cast, unknown cast: ~p~n",[?MODULE,State#state.pool,UnknownCast]),
    {noreply, State}.


%%----------------------

handle_call({get_connection, Timeout}, Client, State0=#state{total_connections=TC, max_total=Max, wait_queue=WQ, wait_queue_len=WQL, connection_request_len=CRL}) ->
    io:format("(~p:~p), handle_call:get_connection~n",[?MODULE,State0#state.pool]),

    case Timeout<now_milli_secs() of
	true -> {reply, timeout, State0};
        _    ->

                 State2 = case get_from_idle_list(State0) of
                              {ok, Connection, State1}  -> deliver_to_client(Connection, Client, State1);
                              _			        -> State1 = case (TC+CRL) < Max of true -> get_extra_connections(1,State0); _ -> State0 end,
                                                           State1#state{wait_queue=queue:in({Client,Timeout},WQ),wait_queue_len=(WQL+1)}
                 end,
                 {noreply, State2}
     end;

handle_call(get_info, _From, State) ->
    {reply, State, State};


handle_call(Request, From, State) ->
    io:format("~p: got call for unknown request ~p from ~p~n",[State#state.pool, Request, From]),
    {reply, error, State}.



%%----------------------

handle_info({'EXIT', Pid, Reason},State0=#state{in_use_map=IUM}) ->
    io:format("(nbdb_pool:handle_info) EXIT from pid ~p for reason ~p~n",[Pid, Reason]),
    %% Pid can be a db connection or a client.
    State = case maps:find(Pid,IUM) of
	{ok, Connections} -> State1 = State0#state{in_use_map=maps:remove(Pid,IUM)},
                             reclaim_connections(Connections,State1);
        _		  -> %% Pid is DB connection
                             cleanup_connection(Pid,State0)
    end,

    {noreply, State};

handle_info(Info, State) ->
    io:format("(nbdb_pool:handle_info) unhandled info: ~p~n",[Info]),
    {noreply, State}.


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
    io:format("pid: ~n~p~n,  ium: ~n~p~n",[Pid,IUM]),
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

    
process_unused_connection(Connection, State=#state{wait_queue=WQ, wait_queue_len=WQL}) ->
    case queue:out(WQ) of
      {empty, _} -> add_to_idle_list(Connection, State);
      {{value, {Client,Timeout}}, NewQ} -> NewWQL=WQL-1,
                                           case Timeout < now_milli_secs() of
			                   true -> process_unused_connection(Connection, State#state{wait_queue=NewQ, wait_queue_len=NewWQL});
                                           _    -> deliver_to_client(Connection,Client,  State#state{wait_queue=NewQ, wait_queue_len=NewWQL})
                end
    end.



recalculate_total_connections(State=#state{in_use_map=IUM,idle_list=Idle}) ->
    State#state{total_connections=(length(Idle) + maps:fold(fun(_Key,Value,AccIn) -> AccIn+length(Value) end, 0, IUM))}.

get_extra_connections(Number, State=#state{connector=PoolConnector, connection_request_len=CRL}) ->
    io:format("get_extra_connections: ~p~n",[Number]),
    nbdb_connector:request(PoolConnector,Number,self()),
    State#state{connection_request_len=CRL+Number}.

now_milli_secs() ->
    erlang:monotonic_time(milli_seconds).

%% reclaim_connecitons occurs when a client using a connection dies
%% Connection is allready taken out of in_use_map
reclaim_connections([], State) ->
    State;
reclaim_connections([Connection|Rest], State=#state{reset_fun=ResetFun}) ->
    io:format("reclaim connnection: ~p~n",[Connection]),
    ResetFun(Connection),
    reclaim_connections(Rest,process_unused_connection(Connection,State)).

%% cleanup_conneciton is called when a db connecition dies
cleanup_connection(Connection, State0=#state{idle_list=IL,in_use_map=IUM, total_connections=TC}) ->
    %% connection can be in idle list (idle_list -> [{Pid,Time},..]) or in in use map (in_use_map)

    %% since a connection has perished, request a new one:
    State=get_extra_connections(1,State0),
   
    NewIL=proplists:delete(Connection,IL),
    NewIUM = maps:filtermap(fun(Client,ConList) -> 
      %% case lists:delete(Connection,ConList) of
       case lists:filter(fun(Item) -> Item =/= Connection end, ConList) of
           ConList -> true;
           []      -> erlang:unlink(Client),
                            %%erlang:exit(Client, db_gone),
                            false;
           NewList -> {true, NewList}
       end
    end, IUM),
    State#state{idle_list=NewIL, in_use_map=NewIUM, total_connections=(TC-1)}.







