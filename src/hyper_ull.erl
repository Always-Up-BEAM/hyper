%% @doc: Using a large binary with ULL
%%
%% This backend uses one plain Erlang binary to store registers. The
%% cost of rebuilding the binary is amortized by keeping a buffer of
%% inserts to perform in the future.
%%
%% It implements the UltraLogLog https://arxiv.org/abs/2308.16862 version,
%% which allows to heavily reduce memory and network size, but also
%% use 8 bytes aligned register, which speeds up operations

-module(hyper_ull).

-behaviour(hyper_register).

%%-compile(native).

-export([
    new/1,
    set/3,
    compact/1,
    max_merge/1, max_merge/2,
    reduce_precision/2,
    bytes/1,
    register_sum/1,
    register_histogram/1,
    zero_count/1,
    encode_registers/1,
    decode_registers/2,
    empty_binary/1,
    precision/1
]).

-define(VALUE_SIZE, 6).
-define(UPDATE_VALUE_SIZE, 2).
-type precision() :: 4..16.

-record(ull, {reg :: binary(), p :: precision()}).

%% TODO: push hash breakdown to the registers, so make it faster
%% TODO: Push card to the backend
new(P) ->
    M = hyper_utils:m(P),
    #ull{reg = empty_binary(M), p = P}.

set(Index, Value, #ull{reg = Reg} = Uul) ->
    NewVal = hyper_utils:run_of_zeroes(Value),
    OldReg = extract(Index, Reg),
    OldPrefix = unpack(OldReg),
    ToPack = OldPrefix bor (1 bsl (NewVal + 1)),
    NewReg = pack(ToPack),
    NewRegs = inject(Index, NewReg, Reg),
    Uul#ull{reg = NewRegs}.

compact(Ull) ->
    Ull.

max_merge([First | Rest]) ->
    lists:foldl(fun max_merge/2, First, Rest).

max_merge(#ull{p = P, reg = RegL}, #ull{p = P, reg = RegR}) ->
    ListL = binary:bin_to_list(RegL),
    ListR = binary:bin_to_list(RegR),
    RetList = lists:zipwith(fun(X, Y) -> pack(unpack(X) bor unpack(Y)) end, ListL, ListR),
    Reg = binary:list_to_bin(RetList),
    #ull{p = P, reg = Reg};
max_merge(#ull{p = Pl} = Ul, #ull{p = Pr} = Ur) when Pl > Pr ->
    max_merge(Ur, Ul);
max_merge(#ull{p = Pl, reg = RegL}, #ull{p = Pr, reg = RegR}) when Pl < Pr ->
    ListL = binary:bin_to_list(RegL),
    ListR = binary:bin_to_list(RegR),
    Mul = Pr - Pl,
    RetReg = lists:reverse(
        lists:foldl(fun fold_register_from_lower_precision/2, {ListR, Mul, []}, ListL)
    ),
    #ull{p = Pl, reg = RetReg}.

reduce_precision(NewP, Ull) ->
    Empty = new(NewP),
    max_merge(Empty, Ull).

register_sum(#ull{reg = Reg}) ->
    List = binary:bin_to_list(Reg),
    lists:foldl(
        fun
            (0, Acc) ->
                Acc + 1.0;
            (1, Acc) ->
                Acc + 0.5;
            (2, Acc) ->
                Acc + 0.25;
            (3, Acc) ->
                Acc + 0.125;
            (4, Acc) ->
                Acc + 0.0625;
            (5, Acc) ->
                Acc + 0.03125;
            (6, Acc) ->
                Acc + 0.015625;
            (7, Acc) ->
                Acc + 0.0078125;
            (8, Acc) ->
                Acc + 0.00390625;
            (9, Acc) ->
                Acc + 0.001953125;
            (V, Acc) ->
                Acc + math:pow(2, -V)
        end,
        0,
        List
    ).

register_histogram(#ull{reg = Reg}) ->
    List = binary:bin_to_list(Reg),
    maps:map(
        fun(_, List1) -> lists:length(List1) end, maps:groups_from_list(fun(Val) -> Val end, List)
    ).

zero_count(#ull{reg = Reg}) ->
    List = binary:bin_to_list(Reg),
    lists:length(lists:filter(fun(X) -> X == 0 end, List)).

encode_registers(#ull{reg = Reg}) ->
    Reg.

decode_registers(AllBytes, P) ->
    M = hyper_utils:m(P),
    Bytes =
        case AllBytes of
            <<B:M/binary>> ->
                B;
            <<B:M/binary, 0>> ->
                B
        end,
    #ull{p = P, reg = Bytes}.

bytes(#ull{reg = Reg}) ->
    erts_debug:flat_size(Reg) * 8.

precision(#ull{p = P}) ->
    P.

%%
%% INTERNALS
%%

empty_binary(M) ->
    binary:copy(<<0:integer>>, M).

extract(Index, Binary) ->
    binary_part(Binary, {Index, 1}).

inject(Index, Value, Binary) ->
    <<LeftPart:(Index - 1)/binary, _:8/integer, RightPart/binary>> = Binary,
    <<LeftPart:(Index - 1)/binary, Value:8/integer, RightPart/binary>>.

unpack(<<Register:8/integer>>) when Register < 4 -> 0;
unpack(<<ShiftInt:?VALUE_SIZE/integer, Val:?UPDATE_VALUE_SIZE/bitstring>>) ->
    <<Ret:64/integer>> =
        <<0:(62 - ShiftInt)/bitstring, Val:?UPDATE_VALUE_SIZE/bitstring, 0:ShiftInt/bitstring>>,
    Ret;
unpack(Register) when is_integer(Register), Register < 4 -> 0;
unpack(Register) when is_integer(Register) ->
    unpack(<<Register:8/integer>>).

pack(Value) ->
    BinVal = <<Value:64/integer>>,
    U = 62 - hyper_utils:run_of_zeroes(0, BinVal),
    <<_:(62 - U), NewShift:?UPDATE_VALUE_SIZE/integer, _/bitstring>> = BinVal,
    4 * U + NewShift.

fold_register_from_lower_precision(Ri, {[Rj | List], P, RetReg}) ->
    Pbsl = 1 bsl P,
    X = unpack(Ri) bor (unpack(Rj) * Pbsl),
    {Jelems, Rest} = lists:split(Pbsl - 1, List),
    Jelems_with_index = lists:enumerate(Jelems),
    ToPack = lists:foldl(
        fun({L, Rjplus}, Xlocal) ->
            case Rjplus of
                0 -> Xlocal;
                _ -> Xlocal bor (1 bsl (hyper_utils:run_of_zeroes(<<L:64/integer>>) + Pbsl - 63))
            end
        end,
        X,
        Jelems_with_index
    ),
    Rret =
        case ToPack of
            0 -> Ri;
            _ -> pack(Ri)
        end,
    {Rest, P, [Rret | RetReg]}.
