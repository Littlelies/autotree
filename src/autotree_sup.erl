%%%-------------------------------------------------------------------
%% @doc autotree top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(autotree_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
    autotree_subs:init_from_sup(),
    Subs = {autotree_subs, {autotree_subs, start_link, []}, permanent, 5000, worker, [autotree_subs]},
    autotree_data:init_from_sup(),
    Data = {autotree_data, {autotree_data, start_link, []}, permanent, 5000, worker, [autotree_data]},
    %% If subs dies, we will just loose all monitors
    %% @todo: when autotree_subs starts, replace all monitors
    %% If data dies, we loose updates in the process mailbox
    {ok, { {one_for_all, 1, 5}, [
        Subs,
        Data
    ]} }.
%%====================================================================
%% Internal functions
%%====================================================================
