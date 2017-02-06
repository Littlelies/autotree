-module(autotree_data).

-behaviour(gen_server).

-export([
    init_from_sup/0,
    update/3,
    browse/2,
    get_timestamp_and_opaque/1
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

-define(ETS_BAG, autotree_data_bag).
-define(ETS_BAG_INDEX, autotree_data_bag_index).
-define(ETS_SET, autotree_data_set).

%% @todo: lookups happen independently, so we should try to group set operations
%% @todo: performance would be improved by reducing the amount of lists:reverse

%% ===================================================================
%% API functions
%% ===================================================================

-spec update([any()], integer(), any()) -> [[any()]] | drop.
update(PathAsList, Timestamp, Opaque) ->
    %% The update must be done one at a time to avoid concurrent writes with bad timestamps
    gen_server:call(?MODULE, {update, PathAsList, Timestamp, Opaque}).

-spec browse([any()], integer()) -> [{[any()], integer(), any()}].
browse(PathAsList, Timestamp) ->
    %% Browse children
    L = ets:lookup(?ETS_BAG, PathAsList),
    browse_items(L, Timestamp, []).

-spec get_timestamp_and_opaque([any()]) -> {integer(), any()} | error.
get_timestamp_and_opaque(PathAsList) ->
    case ets:lookup(?ETS_SET, PathAsList) of
        [{_, Timestamp, Opaque}] ->
            {Timestamp, Opaque};
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
    {ok, #state{}}.

init_from_sup() ->
    ets:new(?ETS_SET, [set, named_table, public]),
    ets:new(?ETS_BAG, [duplicate_bag, named_table, public]), %% See: http://erlang.org/pipermail/erlang-questions/2011-October/061705.html
    ets:new(?ETS_BAG_INDEX, [set, named_table, public]).

handle_call({update, PathAsList, Timestamp, Opaque}, _From, State) ->
    AllPaths = update_at_each_step(lists:reverse(PathAsList), Timestamp, Opaque, true, true, [PathAsList]),
    {reply, AllPaths, State};
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

update_at_each_step(_Elements, _Timestamp, _Opaque, _IsFirst, drop, _Acc) ->
    drop;
update_at_each_step([], _Timestamp, _Opaque, _IsFirst, _ShallContinue, Acc) ->
    Acc;
update_at_each_step([Element | Elements0], Timestamp, Opaque, IsFirst, ShallContinue, Acc) ->
    Elements = lists:reverse(Elements0),
    FullElements = Elements ++ [Element],
    NewShallContinue = case ShallContinue of
        true ->
            case find_child_record(FullElements) of
                false ->
                    if
                        IsFirst ->
                            ets:insert(?ETS_SET, {FullElements, Timestamp, Opaque}),
                            add_in_bag({Elements, Element, Timestamp, 0}, FullElements);
                        true ->
                            add_in_bag({Elements, Element, 0, Timestamp}, FullElements)
                    end,
                    true;
                {Elements, Element, CurrentTimestamp, CurrentChildrenTimestamp} = OldElement ->
                    if
                        IsFirst ->
                            %% Maybe update topic
                            if
                                CurrentTimestamp < Timestamp ->
                                    FullElements = Elements ++ [Element],
                                    ets:insert(?ETS_SET, {FullElements, Timestamp, Opaque}),
                                    add_in_bag({Elements, Element, Timestamp, CurrentChildrenTimestamp}, FullElements),
                                    delete_in_bag(OldElement),
                                    true;
                                CurrentTimestamp =:= Timestamp ->
                                    [{_, _, OldOpaque}] = ets:lookup(?ETS_SET, FullElements),
                                    if
                                        OldOpaque < Opaque ->
                                            FullElements = Elements ++ [Element],
                                            ets:insert(?ETS_SET, {FullElements, Timestamp, Opaque}),
                                            add_in_bag({Elements, Element, Timestamp, CurrentChildrenTimestamp}, FullElements),
                                            delete_in_bag(OldElement),
                                            true;
                                        true ->
                                            %% Another update is more recent at this topic, drop it
                                            drop
                                    end;
                                true ->
                                    %% Another update is more recent at this topic, drop it
                                    drop
                            end;
                        CurrentChildrenTimestamp > Timestamp ->
                            %% The rest is necessarily newer, drop it.
                            %% This use case is DANGEROUS for clients,
                            %% as they may be confused getting events out of order
                            %% We continue the browsing to still alert clients
                            false;
                        true ->
                            add_in_bag({Elements, Element, CurrentTimestamp, Timestamp}, FullElements),
                            delete_in_bag(OldElement),
                            true
                    end
            end;
        false ->
            false
    end,
    update_at_each_step(Elements0, Timestamp, Opaque, false, NewShallContinue, [Elements | Acc]).

browse_items([], _, Acc) ->
    Acc;
browse_items([{Root, Item, ItemTimestamp, ChildrenTimestamp} | Items], Timestamp, Acc) ->
    Acc1 = if
        ItemTimestamp > Timestamp ->
            ItemPath = Root ++ [Item],
            [{_, ItemLatestTimestamp, Opaque}] = ets:lookup(?ETS_SET, ItemPath),
            [{ItemPath, ItemLatestTimestamp, Opaque} | Acc];
        true ->
            Acc
    end,
    Acc2 = if
        ChildrenTimestamp > Timestamp ->
            L = ets:lookup(?ETS_BAG, Root ++ [Item]),
            browse_items(L, Timestamp, Acc1);
        true ->
            Acc1
    end,
    browse_items(Items, Timestamp, Acc2).

add_in_bag({Elements, Element, Timestamp, ChildrenTimestamp}, FullElements) ->
    ets:insert(?ETS_BAG_INDEX, {FullElements, Elements, Element, Timestamp, ChildrenTimestamp}),
    ets:insert(?ETS_BAG, {Elements, Element, Timestamp, ChildrenTimestamp}).

%% we don't remove it from index, it will be overwritten
delete_in_bag(Object) ->
    ets:delete_object(?ETS_BAG, Object).

-spec find_child_record([any()]) -> tuple() | false.
find_child_record(FullElements) ->
    case ets:lookup(?ETS_BAG_INDEX, FullElements) of
        [{FullElements, Elements, Element, Timestamp, ChildrenTimestamp}] ->
            {Elements, Element, Timestamp, ChildrenTimestamp};
        _ ->
            false
    end.
