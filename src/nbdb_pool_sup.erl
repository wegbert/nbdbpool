%%%-------------------------------------------------------------------
%% @doc nbdbpool pool supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(nbdb_pool_sup).

-behaviour(supervisor).

-export([start_link/0, start_child/2]).
-export([init/1]).

-define(SERVER, ?MODULE).

%% API ========================================

start_child(Pool, PoolConnectorArgs) ->
    io:format("(nbdb_pool_sup:start_child/2) ....~n"),
    Res = supervisor:start_child(?SERVER, [Pool, PoolConnectorArgs]),
    io:format("(nbdb_pool_sup:start_child/2) result: ~p~n", [Res]).

%% OTP ===================================================
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    %% use simple_one_for_one and keep a map of poolname->pid?
    SupFlags =
        #{strategy => simple_one_for_one,
          intensity => 10,
          period => 1},
    ChildSpecs =
        [#{id => nbdb_pool,
           start => {nbdb_pool, start_link, []},
           restart => transient,
           shutdown => brutal_kill,
           type => worker,
           module => nbdb_pool}],
    {ok, {SupFlags, ChildSpecs}}.

%% INTERNAL ====================================================
