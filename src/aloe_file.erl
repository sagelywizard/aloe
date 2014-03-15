-module(aloe_file).

-behaviour(gen_server).

-export([start_link/2,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    tab,
    file,
    compact_file,
    keypos,
    pending,
    pending_count,
    fd,
    compacting,
    cardinality,
    compact_delta
}).

start_link(Filename, Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Filename, Options], []).

init([Filename, Options]) ->
    KeyPos = case lists:keyfind(keypos, 1, Options) of
        {keypos, KeyPos0} -> KeyPos0;
        false -> 1
    end,
    {ok, Fd} = file:open(Filename, [read, append, binary]),
    Tab = ets:new(list_to_atom(Filename), Options),
    file:position(Fd, 0),
    ok = read_file(Tab, Fd),
    CompactFile = Filename ++ ".aof",
    {ok, #state{
        file=Filename,
        compact_file=CompactFile,
        compacting=false,
        keypos=KeyPos,
        pending=[],
        pending_count=0,
        fd=Fd,
        tab=Tab,
        cardinality=hyper:new()
    }}.

read_file(Tab, Fd) ->
    case file:read(Fd, 4) of
        {ok, <<Size:32/integer>>} ->
            case file:read(Fd, Size) of
                {ok, Bin} ->
                    Term = binary_to_term(Bin),
                    ets:insert(Tab, Term),
                    read_file(Tab, Fd);
                eof ->
                    throw(unexpected)
            end;
        eof ->
            ok
    end.

handle_cast(_Msg, State) ->
    {noreply, State, 0}.

handle_call({insert, Obj}, From, State) ->
    #state{
        pending=Pending,
        pending_count=Count,
        compact_delta=Delta,
        cardinality=Card,
        keypos=KeyPos
    } = State,
    State0 = State#state{
        pending_count=Count+1,
        pending=[{From, Obj}|Pending],
        compact_delta=Delta+1,
        cardinality=hyper:insert(element(KeyPos, Obj), Card)
    },
    State1 = case State#state.pending_count of
        10 ->
            commit(State0);
        _ ->
            State0
    end,
    {noreply, maybe_compact(State1), 0};

handle_call({lookup, Obj}, _From, State) ->
    Tab = State#state.tab,
    {reply, ets:lookup(Tab, Obj), State, 0}.

handle_info(timeout, State) ->
    {noreply, maybe_compact(commit(State))}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

commit(#state{pending_count=0}=State) ->
    State;
commit(State) ->
    #state{fd=Fd, tab=Tab, pending=Pending} = State,
    Data = lists:flatmap(fun({_, Obj}) ->
        Bin = term_to_binary(Obj),
        Size = iolist_size(Bin),
        [<<Size:32/integer>>, Bin]
    end, Pending),
    file:write(Fd, Data),
    file:sync(Fd),
    lists:map(fun({From, Obj}) ->
        ets:insert(Tab, Obj),
        gen_server:reply(From, true)
    end, Pending),
    State#state{pending=[], pending_count=0}.

maybe_compact(State) ->
    #state{cardinality=Card, compact_delta=Delta} = State,
    case Delta / hyper:card(Card) > 3 of
        Ratio when Ratio > 3 ->
            compact(State);
        _ ->
            State
    end.

compact(State0) ->
    State = State0#state{compacting=true},
    #state{
        tab=Tab,
        compact_file=Filename,
        file=AOF,
        fd=AOFFD
    } = State,
    {ok, Fd} = file:open(Filename, [append, binary]),
    {HLL, Count} = ets:foldl(fun(Obj, {HLL0, Count0}) ->
        Bin = term_to_binary(Obj),
        Size = iolist_size(Bin),
        file:write(Fd, [<<Size:32/integer>>, Bin]),
        {hyper:insert(term_to_binary(element(KeyPos, Obj)), HLL0), Count0+1}
    end, {hyper:new(16), 0}, Tab),
    file:sync(Fd),
    ok = file:close(Fd),
    ok = file:close(AOFFD),
    ok = file:rename(Filename, AOF),
    {ok, NewFd} = file:open(AOF, [read, append, binary]),
    {ok, _} = file:position(NewFd, eof),
    State#state{fd=NewFd}.
