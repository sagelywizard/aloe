-module(aloe).

-export([new/2, insert/2, lookup/2]).

new(Name, Options) ->
    gen_server:call(aloe_manager, {new, Name, Options}).

insert(Tab, Object) ->
    gen_server:call(aloe_manager, {insert, Tab, Object}).

lookup(Tab, Object) ->
    gen_server:call(aloe_manager, {lookup, Tab, Object}).
