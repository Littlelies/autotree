%%%-------------------------------------------------------------------
%% @doc autotree public API
%% @end
%%%-------------------------------------------------------------------

-module(autotree_app).

-behaviour(application).

-export([
    update/3,
    subscribe/3,
    get_timestamp_and_opaque/1
]).

%% Application callbacks
-export([start/2, stop/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%====================================================================
%% API
%%====================================================================
-spec update([any()], integer(), any()) -> too_late | [{[any()], integer()}].
update(PathAsList, Timestamp, Opaque) ->
    case autotree_data:update(PathAsList, Timestamp, Opaque) of
        drop ->
            too_late;
        AllPaths ->
            [autotree_subs:warn_subscribers(Path, {update, PathAsList, Timestamp, Opaque}) || Path <- AllPaths]
    end.

-spec subscribe([any()], integer(), pid()) -> [{[any()], integer(), any()}].
subscribe(PathAsList, Timestamp, Pid) ->
    %% Add the subscriber to the list BEFORE browsing so we don't miss any event
    autotree_subs:add_subscriber(PathAsList, Pid),
    UpdatedSelf = case autotree_data:get_timestamp_and_opaque(PathAsList) of
        {Time, Opaque} ->
            if
                Time > Timestamp ->
                    [{PathAsList, Time, Opaque}];
                true ->
                    []
            end;
        error ->
            []
    end,
    %% Browse current children state
    UpdatedChildren = autotree_data:browse(PathAsList, Timestamp),
    UpdatedSelf ++ UpdatedChildren.

-spec get_timestamp_and_opaque([any()]) -> {integer(), any()} | error.
get_timestamp_and_opaque(PathAsList) ->
    autotree_data:get_timestamp_and_opaque(PathAsList).

start(_StartType, _StartArgs) ->
    autotree_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
-ifdef(TEST).
autotree_subs_test() ->
    application:start(autotree),
    Pid = spawn(fun() -> timer:sleep(100) end),
    autotree_subs:add_subscriber(<<"toto">>, Pid),
    ?assertEqual(lists:sort(ets:tab2list(autotree_subs_bag)), [{Pid,<<"toto">>},{<<"toto">>,Pid}]),
    timer:sleep(150),
    ?assertEqual(ets:tab2list(autotree_subs_bag), []),
    application:stop(autotree).


autotree_data_test() ->
    application:start(autotree),
    update(["toto", "titi", "tata", "item1"], 1, test1),
    update(["toto", "titi", "tata", "item2"], 2, test2),
    update(["toto", "titi", "tata", "item2"], 3, test3),
    update(["toto", "titi", "tata", "item3"], 2, test4),
    update(["toti"], 4, test5),
    ?assertEqual(
        lists:sort(subscribe(["toto", "titi", "tata"], 0, self())),
        [{["toto","titi","tata","item1"],1, test1},
                  {["toto","titi","tata","item2"],3, test3},
                  {["toto","titi","tata","item3"],2, test4}]
    ),
    ?assertEqual(
        lists:sort(subscribe(["toto", "titi", "tata", "item2"], 2, self())),
        [{["toto","titi","tata","item2"],3, test3}]
    ),
    ?assertEqual(
        lists:sort(subscribe(["toto", "titi", "tata", "item2"], 6, self())),
        []
    ),
    ?assertEqual(
        update(["toto", "titi", "tata", "item2"], 2, test5),
        too_late
    ),
    update(["toto", "titi", "tata", "item2"], 4, test6),
    receive
        {update, Path, Time, Opaque} ->
            ?assertEqual(Path, ["toto", "titi", "tata", "item2"]),
            ?assertEqual(Time, 4),
            ?assertEqual(Opaque, test6)
    after 0 ->
        throw(no_message_received)
    end,
    ?assertEqual(
        lists:sort(subscribe([], 0, self())),
        [{["toti"], 4, test5},
                  {["toto", "titi", "tata", "item1"], 1, test1},
                  {["toto", "titi", "tata", "item2"], 4, test6},
                  {["toto", "titi", "tata", "item3"], 2, test4}]),
    application:stop(autotree).
    
-endif.
