-module(aloe_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

-define(CHILD(M, Type),
        {M, {M, start_link, []}, permanent, 5000, Type, [M]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 5, 10}, [?CHILD(aloe_manager, worker),
                                 ?CHILD(aloe_file_sup, supervisor)]}}.
