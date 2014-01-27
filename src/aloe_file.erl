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
    key,
    pending,
    pending_count,
    fd
}).

start_link(Filename, Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Filename, Options], []).

init([Filename, Options]) ->
    Key = case lists:keyfind(keypos, 1, Options) of
        {keypos, Key0} -> Key0;
        false -> none
    end,
    {ok, Fd} = file:open(Filename, [read, append, binary]),
    Tab = ets:new(list_to_atom(binary_to_list(Filename)), Options),
    file:position(Fd, 0),
    ok = read_file(Tab, Fd),
    {ok, #state{file=Filename, key=Key, pending=[], pending_count=0, fd=Fd, tab=Tab}}.

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
    #state{pending=Pending, pending_count=Count} = State,
    State0 = State#state{
        pending_count=Count+1,
        pending=[{From, Obj}|Pending]
    },
    State1 = case State#state.pending_count of
        10 ->
            commit(State0);
        _ ->
            State0
    end,
    {noreply, State1, 0};

handle_call({lookup, Obj}, _From, State) ->
    Tab = State#state.tab,
    {reply, ets:lookup(Tab, Obj), State, 0}.

handle_info(timeout, State) ->
    {noreply, commit(State)}.

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
