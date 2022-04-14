%%%-------------------------------------------------------------------
%% @doc nbdbpool top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(nbdbpool_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

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
    SupFlags =
        #{strategy => one_for_all,
          intensity => 10,
          period => 1},
    ChildSpecs =
        [#{id => nbdb_connector_sup,
           start => {nbdb_connector_sup, start_link, []},
           restart => permanent,
           shutdown => 5000,
           type => supervisor,
           module => nbdb_connector_sup},
         #{id => nbdb_pool_sup,
           start => {nbdb_pool_sup, start_link, []},
           restart => permanent,
           shutdown => 5000,
           type => supervisor,
           module => nbdb_pool_sup}],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
