-module(zaya_leveldb_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([prepare_rollback_roundtrip_test/1, is_persistent_test/1]).

all()->
  [prepare_rollback_roundtrip_test, is_persistent_test].

init_per_suite(Config)->
  {ok, _Apps} = application:ensure_all_started(zaya_leveldb),
  Config.

end_per_suite(_Config)->
  ok = application:stop(zaya_leveldb),
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

is_persistent_test(_Config)->
  ?assertEqual(true, zaya_leveldb:is_persistent()).

with_ref(Config, Fun)->
  Params = #{dir => ?config(db_dir, Config)},
  catch zaya_leveldb:remove(Params),
  Ref = zaya_leveldb:create(Params),
  try
    Fun(Ref)
  after
    ok = zaya_leveldb:close(Ref)
  end.
