-module(aloe_file_sup).

-behaviour(supervisor).

-export([start_link/0, init/1, start_file/2]).

-define(CHILD(M, Type),
        {M, {M, start_link, []}, permanent, 5000, Type, [M]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_file(Filename, Options) ->
    supervisor:start_child(?MODULE, [Filename, Options]).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10}, [?CHILD(aloe_file, worker)]}}.
