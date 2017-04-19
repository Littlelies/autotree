-module(autotree_data).

-behaviour(gen_server).

-export([
    init_from_sup/0,
    update/3,
    browse/2,
    get_iteration_and_opaque/1
]).


-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    iteration
}).

-define(ETS_BAG, autotree_data_bag).
-define(ETS_BAG_INDEX, autotree_data_bag_index).
-define(ETS_SET, autotree_data_set).

%% @todo: lookups happen independently, so we should try to group set operations
%% @todo: performance would be improved by reducing the amount of lists:reverse

%% ===================================================================
%% API functions
%% ===================================================================

-spec update([any()], any(), boolean()) -> {integer(), [[any()]] | drop, any()}.
update(PathAsList, Opaque, FailIfExists) ->
    %% The update must be done one at a time to avoid concurrent writes with bad timestamps
    gen_server:call(?MODULE, {update, PathAsList, Opaque, FailIfExists}).

-spec browse([any()], integer()) -> [{[any()], integer(), any()}].
browse(PathAsList, Iteration) ->
    %% Browse children
    L = ets:lookup(?ETS_BAG, PathAsList),
    browse_items(L, Iteration, []).

-spec get_iteration_and_opaque([any()]) -> {integer(), any()} | error.
get_iteration_and_opaque(PathAsList) ->
    case ets:lookup(?ETS_SET, PathAsList) of
        [{_, Iteration, Opaque}] ->
            {Iteration, Opaque};
        _ ->
            error
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:send_after(60 * 1000, self(), {gc}),
    {ok, #state{iteration = erlang:system_time(microsecond)}}.

init_from_sup() ->
    ets:new(?ETS_SET, [set, named_table, public]),
    ets:new(?ETS_BAG, [duplicate_bag, named_table, public]), %% See: http://erlang.org/pipermail/erlang-questions/2011-October/061705.html
    ets:new(?ETS_BAG_INDEX, [set, named_table, public]).

handle_call({update, PathAsList, Opaque, FailIfExists}, _From, State) ->
    Iteration = State#state.iteration,
    {AllPathsOrDrop, OldOpaque} = update_at_each_step(lists:reverse(PathAsList), Iteration, Opaque, true, [PathAsList], undefined, FailIfExists),
    {reply, {Iteration, AllPathsOrDrop, OldOpaque}, State#state{iteration = Iteration + 1}};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

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

update_at_each_step([], _Iteration, _Opaque, _IsFirst, Acc, AccOpaque, _FailIfExists) ->
    {Acc, AccOpaque};
update_at_each_step([Element | Elements0], Iteration, Opaque, IsFirst, Acc, AccOpaque, FailIfExists) ->
    Elements = lists:reverse(Elements0),
    FullElements = Elements ++ [Element],
    {ShallContinue, CurrentOpaque} = case find_child_record(FullElements) of
        false ->
            if
                IsFirst ->
                    ets:insert(?ETS_SET, {FullElements, Iteration, Opaque}),
                    add_in_bag({Elements, Element, Iteration, 0}, FullElements),
                    {true, error};
                true ->
                    add_in_bag({Elements, Element, 0, Iteration}, FullElements),
                    {true, not_applicable}
            end;
        {Elements, Element, CurrentIteration, CurrentChildrenIteration} = OldElement ->
            if
                IsFirst ->
                    %% Maybe update topic
                    case ets:lookup(?ETS_SET, FullElements) of
                        [] ->
                            FullElements = Elements ++ [Element],
                            ets:insert(?ETS_SET, {FullElements, Iteration, Opaque}),
                            add_in_bag({Elements, Element, Iteration, 0}, FullElements),
                            {true, error};
                        [{_, _OldIteration, OldOpaque}] ->
                            if
                                OldOpaque < Opaque -> % http://erlang.org/doc/reference_manual/expressions.html#id81088
                                    case FailIfExists of
                                        true ->
                                            dont_update;
                                        false ->                                    
                                            FullElements = Elements ++ [Element],
                                            ets:insert(?ETS_SET, {FullElements, Iteration, Opaque}),
                                            add_in_bag({Elements, Element, Iteration, CurrentChildrenIteration}, FullElements),
                                            delete_in_bag(OldElement)
                                    end,
                                    {true, OldOpaque};
                                true ->
                                    %% Another update is more recent at this topic, drop it
                                    {drop, OldOpaque}
                            end
                    end;
                true ->
                    add_in_bag({Elements, Element, CurrentIteration, Iteration}, FullElements),
                    delete_in_bag(OldElement),
                    {true, not_applicable}
            end
    end,
    NewOldOpaque = case CurrentOpaque of
        not_applicable ->
            AccOpaque;
        _ ->
            CurrentOpaque
    end,
    case ShallContinue of
        drop ->
            {drop, NewOldOpaque};
        true ->
            update_at_each_step(Elements0, Iteration, Opaque, false, [Elements | Acc], NewOldOpaque, FailIfExists)
    end.

browse_items([], _, Acc) ->
    Acc;
browse_items([{Root, Item, ItemIteration, ChildrenIteration} | Items], Iteration, Acc) ->
    Acc1 = if
        ItemIteration > Iteration ->
            ItemPath = Root ++ [Item],
            [{_, OldIteration, Opaque}] = ets:lookup(?ETS_SET, ItemPath),
            [{ItemPath, OldIteration, Opaque} | Acc];
        true ->
            Acc
    end,
    Acc2 = if
        ChildrenIteration > Iteration ->
            L = ets:lookup(?ETS_BAG, Root ++ [Item]),
            browse_items(L, Iteration, Acc1);
        true ->
            Acc1
    end,
    browse_items(Items, Iteration, Acc2).

add_in_bag({Elements, Element, Iteration, ChildrenIteration}, FullElements) ->
    ets:insert(?ETS_BAG_INDEX, {FullElements, Elements, Element, Iteration, ChildrenIteration}),
    ets:insert(?ETS_BAG, {Elements, Element, Iteration, ChildrenIteration}).

%% we don't remove it from index, it will be overwritten
delete_in_bag(Object) ->
    ets:delete_object(?ETS_BAG, Object).

-spec find_child_record([any()]) -> tuple() | false.
find_child_record(FullElements) ->
    case ets:lookup(?ETS_BAG_INDEX, FullElements) of
        [{FullElements, Elements, Element, Iteration, ChildrenIteration}] ->
            {Elements, Element, Iteration, ChildrenIteration};
        _ ->
            false
    end.
