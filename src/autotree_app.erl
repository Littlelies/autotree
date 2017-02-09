%%%-------------------------------------------------------------------
%% @doc autotree public API
%% @end
%%%-------------------------------------------------------------------

-module(autotree_app).

-behaviour(application).

-export([
    update/2,
    subscribe/3,
    get_iteration_and_opaque/1
]).

%% Application callbacks
-export([start/2, stop/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type iteration() :: integer().
-export_type([iteration/0]).
%%====================================================================
%% API
%%====================================================================

%% Updates the tree, warns the subscribers.
%% WARNING: susbscribers may receive update notifications out of order for performance sake
%% it is their responsibility to make sure the event is not outdated already,
%% and make sure they use margin when asking for a timestamp after deconnexion

%% @todo: instead of timestamp, use a version number, so we can make sure it is monotonically increasing
-spec update([any()], any()) -> too_late | {iteration(), [{[any()], integer()}]}.
update(PathAsList, Opaque) ->
    case autotree_data:update(PathAsList, Opaque) of
        {_Iteration, drop} ->
            too_late;
        {Iteration, AllPaths} ->
            {Iteration, [autotree_subs:warn_subscribers(Path, {update, PathAsList, Iteration, Opaque}) || Path <- AllPaths]}
    end.

-spec subscribe([any()], iteration(), pid()) -> [{[any()], iteration(), any()}].
subscribe(PathAsList, Iteration, Pid) ->
    %% Add the subscriber to the list BEFORE browsing so we don't miss any event
    autotree_subs:add_subscriber(PathAsList, Pid),
    UpdatedSelf = case autotree_data:get_iteration_and_opaque(PathAsList) of
        {LastIteration, Opaque} ->
            if
                LastIteration > Iteration ->
                    [{PathAsList, LastIteration, Opaque}];
                true ->
                    []
            end;
        error ->
            []
    end,
    %% Browse current children state
    UpdatedChildren = autotree_data:browse(PathAsList, Iteration),
    UpdatedSelf ++ UpdatedChildren.

-spec get_iteration_and_opaque([any()]) -> {iteration(), any()} | error.
get_iteration_and_opaque(PathAsList) ->
    autotree_data:get_iteration_and_opaque(PathAsList).

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
    Pid = spawn(fun() -> timer:sleep(200) end),
    autotree_subs:add_subscriber([<<"toto">>], Pid),
    ?assertEqual(lists:sort(ets:tab2list(autotree_subs_bag)), [{Pid,[<<"toto">>]},{[<<"toto">>],Pid}]),
    timer:sleep(100),
    {It, _} = update([<<"toto">>], {1, test1}),
    timer:sleep(120),
    ?assertEqual(ets:tab2list(autotree_subs_bag), []),
    ?assertEqual({It, {1, test1}}, get_iteration_and_opaque([<<"toto">>])),
    application:stop(autotree).


autotree_data_test() ->
    application:start(autotree),
    {It, _ } = update(["toto", "titi", "tata", "item1"], {1, test1}),
    update(["toto", "titi", "tata", "item2"], {2, test2}),
    update(["toto", "titi", "tata", "item2"], {3, test3}),
    update(["toto", "titi", "tata", "item3"], {2, test4}),
    update(["toti"], {4, test5}),
    ?assertEqual(
        [{["toto","titi","tata","item1"], It, {1, test1}},
                  {["toto","titi","tata","item2"], It+2, {3, test3}},
                  {["toto","titi","tata","item3"], It+3, {2, test4}}],
        lists:sort(subscribe(["toto", "titi", "tata"], 0, self()))                  
    ),
    ?assertEqual(
        [{["toto","titi","tata","item2"], It+2, {3, test3}}],
        lists:sort(subscribe(["toto", "titi", "tata", "item2"], 0, self()))
    ),
    ?assertEqual(
        [],
        lists:sort(subscribe(["toto", "titi", "tata", "item2"], It+2, self()))
    ),
    ?assertEqual(
        update(["toto", "titi", "tata", "item2"], {2, test5}),
        too_late
    ),
    update(["toto", "titi", "tata", "item2"], {4, test6}),
    receive
        {update, Path, NewIt, Opaque} ->
            ?assertEqual(Path, ["toto", "titi", "tata", "item2"]),
            ?assertEqual(NewIt, It+6),
            ?assertEqual(Opaque, {4, test6})
    after 0 ->
        throw(no_message_received)
    end,
    Subs = subscribe([], 0, self()),
    ?assertEqual(
        lists:sort(Subs),
        [{["toti"], It+4, {4, test5}},
                  {["toto", "titi", "tata", "item1"], It, {1, test1}},
                  {["toto", "titi", "tata", "item2"], It+6, {4, test6}},
                  {["toto", "titi", "tata", "item3"], It+3, {2, test4}}]),

    {It2, _} = update(["toto", "titi"], {4, test5}),
    ?assertEqual(It+7, It2),

    application:stop(autotree).
    
-endif.
