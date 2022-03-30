%%%-------------------------------------------------------------------
%% @doc nbdbpool public API
%% @end
%%%-------------------------------------------------------------------

-module(nbdbpool_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    nbdbpool_sup:start_link().
    %% Read config and add workers!



stop(_State) ->
    ok.

%% internal functions
