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
    max_registers/1,
    precision/1
]).

-define(VALUE_SIZE, 6).
-define(UPDATE_VALUE_SIZE, 2).
-type precision() :: 4..16.

-record(ull, {reg :: binary(), p :: precision()}).

%% TODO: push hash breakdown to the registers, so make it faster
%% TODO: merge with different size
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
    binary:list_to_bin(RetList);
max_merge(
    #buffer{buf = Buf, buf_size = BufferSize},
    #dense{buf = DenseBuf, buf_size = DenseSize} = Dense
) ->
    case BufferSize + DenseSize < Dense#dense.merge_threshold of
        true ->
            Dense#dense{buf = Buf ++ DenseBuf, buf_size = BufferSize + DenseSize};
        false ->
            Merged = max_registers(DenseBuf ++ Buf),
            Dense#dense{buf = Merged, buf_size = length(Merged)}
    end;
max_merge(#dense{} = Dense, #buffer{} = Buffer) ->
    max_merge(Buffer, Dense);
max_merge(
    #buffer{buf = LeftBuf, buf_size = LeftBufSize},
    #buffer{buf = RightBuf, buf_size = RightBufSize} = Right
) ->
    case LeftBufSize + RightBufSize < Right#buffer.convert_threshold of
        true ->
            Right#buffer{buf = LeftBuf ++ RightBuf, buf_size = LeftBufSize + RightBufSize};
        false ->
            Merged = max_registers(LeftBuf ++ RightBuf),
            NewRight = Right#buffer{buf = Merged, buf_size = length(Merged)},
            case NewRight#buffer.buf_size < NewRight#buffer.convert_threshold of
                true ->
                    NewRight;
                false ->
                    buffer2dense(NewRight)
            end
    end.

reduce_precision(NewP, #dense{p = OldP} = Dense) ->
    ChangeP = OldP - NewP,
    Buf = register_fold(ChangeP, Dense),
    Empty = new_dense(NewP),
    compact(Empty#dense{buf = Buf});
reduce_precision(NewP, #buffer{p = OldP} = Buffer) ->
    ChangeP = OldP - NewP,
    Buf = register_fold(ChangeP, Buffer),
    Empty = new_dense(NewP),
    compact(Empty#dense{buf = Buf}).

%% fold an HLL to a lower number of registers `NewM` by projecting the values
%% onto their new index, see
%% http://research.neustar.biz/2012/09/12/set-operations-on-hlls-of-different-sizes/
%% NOTE: this function does not perform the max_registers step
register_fold(ChangeP, B) ->
    ChangeM = hyper_utils:m(ChangeP),
    element(
        3,
        fold(
            fun
                (I, V, {_Index, CurrentList, Acc}) when CurrentList == [] ->
                    {I, [V], Acc};
                (I, V, {Index, CurrentList, Acc}) when I - Index < ChangeM ->
                    {Index, [V | CurrentList], Acc};
                (I, V, {_Index, CurrentList, Acc}) ->
                    {I, [V], [
                        {I bsr ChangeP, hyper_utils:changeV(lists:reverse(CurrentList), ChangeP)}
                        | Acc
                    ]}
            end,
            {0, [], []},
            B
        )
    ).

register_sum(B) ->
    fold(
        fun
            (_, 0, Acc) ->
                Acc + 1.0;
            (_, 1, Acc) ->
                Acc + 0.5;
            (_, 2, Acc) ->
                Acc + 0.25;
            (_, 3, Acc) ->
                Acc + 0.125;
            (_, 4, Acc) ->
                Acc + 0.0625;
            (_, 5, Acc) ->
                Acc + 0.03125;
            (_, 6, Acc) ->
                Acc + 0.015625;
            (_, 7, Acc) ->
                Acc + 0.0078125;
            (_, 8, Acc) ->
                Acc + 0.00390625;
            (_, 9, Acc) ->
                Acc + 0.001953125;
            (_, V, Acc) ->
                Acc + math:pow(2, -V)
        end,
        0,
        B
    ).

register_histogram(#buffer{p = P} = B) ->
    register_histogram(B, P);
register_histogram(#dense{p = P} = B) ->
    register_histogram(B, P).

register_histogram(B, P) ->
    % todo use from_keys once we are at otp 26
    fold(
        fun(_, Value, Acc) -> maps:update_with(Value, fun(V) -> V + 1 end, 0, Acc) end,
        maps:from_list(
            lists:map(fun(I) -> {I, 0} end, lists:seq(0, 65 - P))
        ),
        B
    ).

zero_count(B) ->
    fold(
        fun
            (_, 0, Acc) ->
                Acc + 1;
            (_, _, Acc) ->
                Acc
        end,
        0,
        B
    ).

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
    Dense = new_dense(P),
    Dense#dense{b = <<<<I:?VALUE_SIZE/integer>> || <<I:8>> <= Bytes>>}.

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
    <<LeftPart:(Index - 1)/binary, _:1/binary, RightPart/binary>> = Binary,
    <<LeftPart:(Index - 1)/binary, Value:8/integer, RightPart/binary>>.

max_registers(Buf) ->
    lists:keysort(
        1,
        maps:to_list(
            lists:foldl(
                fun({I, V}, Acc) ->
                    case maps:find(I, Acc) of
                        {ok, R} when R >= V -> Acc;
                        _ -> maps:put(I, V, Acc)
                    end
                end,
                #{},
                Buf
            )
        )
    ).

do_merge(<<>>, <<>>, Acc) ->
    Acc;
do_merge(
    <<Left:?VALUE_SIZE/integer, SmallRest/bitstring>>,
    <<Right:?VALUE_SIZE/integer, BigRest/bitstring>>,
    Acc
) ->
    do_merge(SmallRest, BigRest, <<Acc/bits, (max(Left, Right)):?VALUE_SIZE>>).

fold(F, Acc, #buffer{} = Buffer) ->
    % TODO fix this to not need to move to a dense, it is not technically neeeded
    % We can just fold on the list i think
    fold(F, Acc, buffer2dense(Buffer));
fold(F, Acc, #dense{b = B, buf = Buf}) ->
    do_fold(F, Acc, merge_buf(B, max_registers(Buf)), 0).

do_fold(_, Acc, <<>>, _) ->
    Acc;
do_fold(F, Acc, <<Value:?VALUE_SIZE/integer, Rest/bitstring>>, Index) ->
    do_fold(F, F(Index, Value, Acc), Rest, Index + 1).

merge_buf(B, L) ->
    merge_buf(B, L, -1, <<>>).

merge_buf(B, [], _PrevIndex, Acc) ->
    <<Acc/bitstring, B/bitstring>>;
merge_buf(B, [{Index, Value} | Rest], PrevIndex, Acc) ->
    I = (Index - PrevIndex - 1) * ?VALUE_SIZE,
    case B of
        <<Left:I/bitstring, OldValue:?VALUE_SIZE/integer, Right/bitstring>> ->
            case OldValue < Value of
                true ->
                    NewAcc = <<Acc/bitstring, Left/bitstring, Value:?VALUE_SIZE/integer>>,
                    merge_buf(Right, Rest, Index, NewAcc);
                false ->
                    NewAcc = <<Acc/bitstring, Left/bitstring, OldValue:?VALUE_SIZE/integer>>,
                    merge_buf(Right, Rest, Index, NewAcc)
            end;
        <<Left:I/bitstring>> ->
            <<Acc/bitstring, Left/bitstring, Value:?VALUE_SIZE/integer>>
    end.

unpack(Register) when Register < 4 -> 0;
unpack(<<ShiftInt:6/integer, Val:2/bitstring>>) ->
    <<Ret:64/integer>> = <<0:(62 - ShiftInt)/bitstring, Val:2/bitstring, 0:ShiftInt/bitstring>>,
    Ret.

pack(Value) ->
    BinVal = <<Value:64/integer>>,
    U = 62 - hyper_utils:run_of_zeroes(0, BinVal),
    <<_:(62 - U), NewShift:2/integer, _/bitstring>> = BinVal,
    4 * U + NewShift.
