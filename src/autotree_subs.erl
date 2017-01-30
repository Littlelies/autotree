-module(autotree_subs).

-behaviour(gen_server).

-export([
    init_from_sup/0,
    add_subscriber/2,
    warn_subscribers/2
]).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-define(ETS_SUBS_BAG, autotree_subs_bag).

%% ===================================================================
%% API functions
%% ===================================================================

init_from_sup() ->
    ets:new(?ETS_SUBS_BAG, [bag, named_table, public]).


add_subscriber(Path, Pid) ->
    gen_server:call(?MODULE, {add_sub, Path, Pid}).

-spec warn_subscribers([any()], any()) -> {[any()], integer()}.
warn_subscribers(Path, Message) ->
    Subs = ets:lookup(?ETS_SUBS_BAG, Path),
    {Path, warn_each_sub(Subs, Message, 0)}.

%%====================================================================
%% gen_server callbacks
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:send_after(60 * 1000, self(), {gc}),    
    {ok, #state{}}.

handle_call({add_sub, Path, Pid}, _From, State) ->
    erlang:monitor(process, Pid),
    ets:insert(?ETS_SUBS_BAG, [{Pid, Path}, {Path, Pid}]),    
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MonitorRef, process, Pid, _Reason}, State) ->
    [delete_from_ets(Path, Pid) || {_, Path} <- ets:lookup(?ETS_SUBS_BAG, Pid)],
    {noreply, State};
handle_info({gc}, State) ->
    erlang:garbage_collect(self()),
    erlang:send_after(60 * 1000, self(), {gc}),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

delete_from_ets(Path, Pid) ->
    ets:delete_object(?ETS_SUBS_BAG, {Path, Pid}),
    ets:delete_object(?ETS_SUBS_BAG, {Pid, Path}).

warn_each_sub([], _Message, Count) ->
    Count;
warn_each_sub([{_, Pid} | Subs], Message, Count) ->
    Pid ! Message,
    warn_each_sub(Subs, Message, Count + 1).


