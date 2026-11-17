defmodule Spectre.Pulse.PhoenixMacroTest do
  use ExUnit.Case, async: true

  test "the optional Phoenix host adapter rejects missing or malformed connection options" do
    for opts <- [[], [transport: :websocket], ["studio"], :studio] do
      assert_raise ArgumentError, ~r/keyword list containing :connection/, fn ->
        Code.compile_quoted(
          quote do
            defmodule InvalidPulsePhoenixHost do
              use Spectre.Pulse.Phoenix, unquote(Macro.escape(opts))
            end
          end
        )
      end
    end
  end
end
