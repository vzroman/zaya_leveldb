-module(zaya_leveldb_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(POOL_PARAMS, #{
  pool => #{
    pool_size => 1,
    batch_size => 4
  }
}).

-export([
  all/0,
  groups/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_group/2,
  end_per_group/2,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  default_pool_created_test/1,
  service_api_and_info_test/1,
  low_level_api_test/1,
  iterator_navigation_test/1,
  find_query_variants_test/1,
  fold_and_copy_test/1,
  dump_batch_test/1,
  transaction_api_test/1,
  prepare_rollback_roundtrip_test/1,
  prepare_rollback_unchanged_write_test/1,
  is_persistent_test/1,
  concurrent_write_callers_test/1
]).

all()->
  [
    default_pool_created_test,
    {group, pool_mode},
    {group, direct_mode}
  ].

groups()->
  Tests = mode_tests(),
  [
    {pool_mode, [sequence], Tests},
    {direct_mode, [sequence], Tests}
  ].

mode_tests()->
  [
    service_api_and_info_test,
    low_level_api_test,
    iterator_navigation_test,
    find_query_variants_test,
    fold_and_copy_test,
    dump_batch_test,
    transaction_api_test,
    prepare_rollback_roundtrip_test,
    prepare_rollback_unchanged_write_test,
    is_persistent_test,
    concurrent_write_callers_test
  ].

init_per_suite(Config)->
  {ok, _Apps} = application:ensure_all_started(zaya_leveldb),
  Config.

end_per_suite(_Config)->
  ok = application:stop(zaya_leveldb),
  ok.

init_per_group(pool_mode, Config)->
  [{mode, pool}, {mode_params, ?POOL_PARAMS} | Config];
init_per_group(direct_mode, Config)->
  [{mode, direct}, {mode_params, #{pool => disabled}} | Config];
init_per_group(_Group, Config)->
  Config.

end_per_group(_Group, _Config)->
  ok.

init_per_testcase(TestCase, Config)->
  Dir =
    filename:join(
      ?config(priv_dir, Config),
      atom_to_list(TestCase) ++ "_" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
  [{db_dir, Dir} | Config].

end_per_testcase(_TestCase, Config)->
  Dir = ?config(db_dir, Config),
  catch zaya_leveldb:remove(#{dir => Dir}),
  ok.

%%=================================================================
%%  TESTS
%%=================================================================
default_pool_created_test(Config)->
  Params = #{dir => ?config(db_dir, Config)},
  catch zaya_leveldb:remove(Params),
  Ref = zaya_leveldb:create(Params),
  try
    ?assert(is_pid(ref_pool(Ref))),
    ok = zaya_leveldb:write(Ref, [{default_key, default_val}]),
    ?assertEqual([{default_key, default_val}], zaya_leveldb:read(Ref, [default_key]))
  after
    ok = zaya_leveldb:close(Ref)
  end.

service_api_and_info_test(Config)->
  Params = backend_params(Config),
  catch zaya_leveldb:remove(Params),

  Ref1 = zaya_leveldb:create(Params),
  try
    assert_mode_ref(Config, Ref1),
    ?assertEqual(undefined, zaya_leveldb:first(Ref1)),
    ?assertEqual(undefined, zaya_leveldb:last(Ref1)),
    ok = zaya_leveldb:write(Ref1, [{service_key, service_value}]),
    ?assert(zaya_leveldb:get_size(Ref1) > 0)
  after
    ok = zaya_leveldb:close(Ref1)
  end,

  Ref2 = zaya_leveldb:open(Params),
  try
    assert_mode_ref(Config, Ref2),
    ?assertEqual(
      [{service_key, service_value}],
      zaya_leveldb:read(Ref2, [service_key])
    )
  after
    ok = zaya_leveldb:close(Ref2)
  end,

  ?assertEqual(ok, zaya_leveldb:remove(Params)).

low_level_api_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_leveldb:write(Ref, sample_records()),
      ?assertEqual(
        [{5, five}, {1, one}, {3, three}],
        zaya_leveldb:read(Ref, [5, 1, 99, 3])
      ),

      ok = zaya_leveldb:write(Ref, []),
      ok = zaya_leveldb:delete(Ref, [3, 42]),
      ?assertEqual(
        #{1 => one, 5 => five, 7 => seven},
        read_map(Ref, [1, 5, 7])
      ),

      ok = zaya_leveldb:delete(Ref, []),
      ?assertEqual([], zaya_leveldb:read(Ref, [3, 42]))
    end
  ).

iterator_navigation_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ?assertEqual(undefined, zaya_leveldb:first(Ref)),
      ?assertEqual(undefined, zaya_leveldb:last(Ref)),
      ?assertEqual(undefined, zaya_leveldb:next(Ref, 1)),
      ?assertEqual(undefined, zaya_leveldb:prev(Ref, 1)),

      ok = zaya_leveldb:write(Ref, sample_records()),
      ?assertEqual({1, one}, zaya_leveldb:first(Ref)),
      ?assertEqual({7, seven}, zaya_leveldb:last(Ref)),
      ?assertEqual({3, three}, zaya_leveldb:next(Ref, 1)),
      ?assertEqual({3, three}, zaya_leveldb:next(Ref, 2)),
      ?assertEqual(undefined, zaya_leveldb:next(Ref, 7)),
      ?assertEqual({5, five}, zaya_leveldb:prev(Ref, 7)),
      ?assertEqual({5, five}, zaya_leveldb:prev(Ref, 6)),
      ?assertEqual(undefined, zaya_leveldb:prev(Ref, 1))
    end
  ).

find_query_variants_test(Config)->
  MSAtLeastThree = [{{'$1', '$2'}, [{'>=', '$1', 3}], ['$_']}],
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_leveldb:write(Ref, sample_records()),

      ?assertEqual(sample_records(), zaya_leveldb:find(Ref, #{})),
      ?assertEqual(
        [{3, three}, {5, five}, {7, seven}],
        zaya_leveldb:find(Ref, #{start => 2})
      ),
      ?assertEqual(
        [{1, one}, {3, three}, {5, five}],
        zaya_leveldb:find(Ref, #{stop => 5})
      ),
      ?assertEqual(
        [{3, three}, {5, five}],
        zaya_leveldb:find(Ref, #{start => 2, stop => 5})
      ),
      ?assertEqual(
        [{1, one}, {3, three}],
        zaya_leveldb:find(Ref, #{limit => 2})
      ),
      ?assertEqual(
        [{3, three}, {5, five}],
        zaya_leveldb:find(Ref, #{start => 2, stop => 5, limit => 2})
      ),
      ?assertEqual(
        [{3, three}, {5, five}, {7, seven}],
        zaya_leveldb:find(Ref, #{ms => MSAtLeastThree})
      ),
      ?assertEqual(
        [{3, three}, {5, five}],
        zaya_leveldb:find(Ref, #{ms => MSAtLeastThree, limit => 2})
      ),
      ?assertEqual(
        [{3, three}, {5, five}],
        zaya_leveldb:find(Ref, #{start => 2, stop => 5, ms => MSAtLeastThree})
      ),
      ?assertEqual(
        [{3, three}],
        zaya_leveldb:find(
          Ref,
          #{start => 2, stop => 7, ms => MSAtLeastThree, limit => 1}
        )
      )
    end
  ).

fold_and_copy_test(Config)->
  MSValuesAtLeastThree = [{{'$1', '$2'}, [{'>=', '$1', 3}], ['$2']}],
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_leveldb:write(Ref, sample_records()),

      ?assertEqual(
        [seven, five, three],
        zaya_leveldb:foldl(
          Ref,
          #{ms => MSValuesAtLeastThree},
          fun(Value, Acc)-> [Value | Acc] end,
          []
        )
      ),

      ?assertEqual(
        [1, 3, 5],
        zaya_leveldb:foldr(
          Ref,
          #{start => 5},
          fun({Key, _Value}, Acc)-> [Key | Acc] end,
          []
        )
      ),

      ?assertEqual(
        [five, three, one],
        zaya_leveldb:foldl(
          Ref,
          #{stop => 5},
          fun({_K, V}, Acc)-> [V | Acc] end,
          []
        )
      ),

      ?assertEqual(
        [three, five, seven],
        zaya_leveldb:foldr(
          Ref,
          #{stop => 3},
          fun({_K, V}, Acc)-> [V | Acc] end,
          []
        )
      ),

      ?assertEqual(
        [{5, five}, {3, three}, {1, one}],
        zaya_leveldb:foldl(
          Ref,
          #{},
          fun({5, five} = Rec, Acc)-> throw({stop, [Rec | Acc]});
             (Rec, Acc)-> [Rec | Acc]
          end,
          []
        )
      ),

      CopyResult = zaya_leveldb:copy(
        Ref,
        fun({K, V}, Acc)-> [{sext:decode(K), binary_to_term(V)} | Acc] end,
        []
      ),
      ?assertEqual(
        [{7, seven}, {5, five}, {3, three}, {1, one}],
        CopyResult
      )
    end
  ).

dump_batch_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      RawBatch = [
        {sext:encode(raw_a), term_to_binary(10)},
        {sext:encode(raw_b), term_to_binary(20)}
      ],
      ok = zaya_leveldb:dump_batch(Ref, RawBatch),
      ?assertEqual(
        #{raw_a => 10, raw_b => 20},
        read_map(Ref, [raw_a, raw_b])
      )
    end
  ).

transaction_api_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_leveldb:write(Ref, [{keep, 1}, {drop, 2}, {stay, 3}]),

      ok = zaya_leveldb:commit(Ref, [{keep, 10}, {add, 11}], [drop]),
      ?assertEqual(
        #{keep => 10, add => 11, stay => 3},
        read_map(Ref, [keep, add, stay])
      ),
      ?assertEqual([], zaya_leveldb:read(Ref, [drop])),

      ok = zaya_leveldb:commit(Ref, [], [])
    end
  ).

prepare_rollback_roundtrip_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_leveldb:write(Ref, [{item, original}]),
      {RollbackWrite, RollbackDelete} =
        zaya_leveldb:prepare_rollback(Ref, [{item, updated}], []),
      ok = zaya_leveldb:commit(Ref, [{item, updated}], []),
      ok = zaya_leveldb:commit(Ref, RollbackWrite, RollbackDelete),
      ?assertEqual([{item, original}], zaya_leveldb:read(Ref, [item]))
    end
  ).

prepare_rollback_unchanged_write_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_leveldb:write(Ref, [{same, value}, {delete_me, doomed}]),
      {RollbackWrite, RollbackDelete} =
        zaya_leveldb:prepare_rollback(
          Ref,
          [{same, value}, {new_key, new_value}],
          [delete_me, missing_key]
        ),
      ?assertEqual([{delete_me, doomed}], RollbackWrite),
      ?assertEqual([new_key], RollbackDelete)
    end
  ).

is_persistent_test(_Config)->
  ?assertEqual(true, zaya_leveldb:is_persistent()).

concurrent_write_callers_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      Parent = self(),
      Keys = lists:seq(1, 8),
      [
        spawn(fun()->
          Result = catch zaya_leveldb:write(Ref, [{Key, Key * 10}]),
          Parent ! {writer_result, Key, Result}
        end)
       || Key <- Keys
      ],
      Results = collect_results(length(Keys), []),
      ?assertEqual([], [Result || {_Key, Result} <- Results, Result =/= ok]),
      ?assertEqual(
        maps:from_list([{Key, Key * 10} || Key <- Keys]),
        read_map(Ref, Keys)
      )
    end
  ).

%%=================================================================
%%  HELPERS
%%=================================================================
with_ref(Config, Fun)->
  Params = backend_params(Config),
  catch zaya_leveldb:remove(Params),
  Ref = zaya_leveldb:create(Params),
  try
    assert_mode_ref(Config, Ref),
    Fun(Ref)
  after
    ok = zaya_leveldb:close(Ref)
  end.

backend_params(Config)->
  ModeParams = ?config(mode_params, Config),
  Base = #{dir => ?config(db_dir, Config)},
  case ModeParams of
    undefined -> Base;
    _ -> maps:merge(ModeParams, Base)
  end.

assert_mode_ref(Config, Ref)->
  case ?config(mode, Config) of
    pool ->
      ?assert(is_pid(ref_pool(Ref)));
    direct ->
      ?assertEqual(disabled, ref_pool(Ref));
    _ ->
      ok
  end.

ref_pool(Ref)->
  element(tuple_size(Ref), Ref).

sample_records()->
  [{1, one}, {3, three}, {5, five}, {7, seven}].

read_map(Ref, Keys)->
  maps:from_list(zaya_leveldb:read(Ref, Keys)).

collect_results(0, Results)->
  Results;
collect_results(Count, Results)->
  receive
    {writer_result, Key, Result}->
      collect_results(Count - 1, [{Key, Result} | Results])
  after
    5000 ->
      ct:fail(timeout)
  end.
