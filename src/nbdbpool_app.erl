%%%-------------------------------------------------------------------
%% @doc nbdbpool public API
%% @end
%%%-------------------------------------------------------------------

-module(nbdbpool_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    Pid = nbdbpool_sup:start_link(),
    %% Read config and add workers!
    {ok,ConfigPools} = application:get_env(pools),
    nbdb:start_pools(ConfigPools),
    Pid.

stop(_State) ->
    ok.

%% internal functions

